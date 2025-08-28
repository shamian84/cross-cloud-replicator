# src/app.py
import os
import logging
import time
from typing import Optional

import boto3
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

# Absolute imports (works reliably when src is a package)
from src.storage.adapters import S3Adapter, LocalGCSAdapter
from src.replicator import replicate_object

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------
# Config via environment variables
# ----------------------------
GCS_BASE_PATH = os.environ.get("LOCAL_GCS_PATH", "/tmp/local_gcs")
DEFAULT_TARGET_GCS_BUCKET = os.environ.get("TARGET_GCS_BUCKET", "replica-bucket")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
RETRY_DELAY = float(os.environ.get("RETRY_DELAY", 0.1))  # seconds

# ----------------------------
# Default adapters
# ----------------------------
_s3_client = boto3.client("s3", region_name="us-east-1")
_default_s3_adapter = S3Adapter(_s3_client)
_default_gcs_adapter = LocalGCSAdapter(GCS_BASE_PATH)

# ----------------------------
# FastAPI App
# ----------------------------
app = FastAPI(title="Cross-cloud Replicator (local-mode)")

# ----------------------------
# Dependency injection
# ----------------------------
def get_s3_adapter() -> S3Adapter:
    return _default_s3_adapter

def get_gcs_adapter() -> LocalGCSAdapter:
    return _default_gcs_adapter

# ----------------------------
# Request / Response Models
# ----------------------------
class ReplicationRequest(BaseModel):
    src_bucket: str
    src_key: str
    dest_bucket: Optional[str] = None
    dest_key: Optional[str] = None

class ReplicationResult(BaseModel):
    bucket: str
    key: str
    status: str
    size: Optional[int] = None

class ReplicationResponse(BaseModel):
    source: ReplicationResult
    destination: ReplicationResult
    result: ReplicationResult

class DeleteResponse(BaseModel):
    bucket: str
    key: str
    status: str

class HealthResponse(BaseModel):
    status: str

class RootResponse(BaseModel):
    message: str

# ----------------------------
# Routes
# ----------------------------
@app.post("/v1/replicate", response_model=ReplicationResponse)
def replicate_endpoint(
    payload: ReplicationRequest,
    s3_adapter: S3Adapter = Depends(get_s3_adapter),
    gcs_adapter: LocalGCSAdapter = Depends(get_gcs_adapter),
):
    """Replicate object from S3 to GCS-like storage with retries."""
    target_bucket = payload.dest_bucket or DEFAULT_TARGET_GCS_BUCKET
    target_key = payload.dest_key or payload.src_key

    logger.info(f"Replication requested: {payload.src_bucket}/{payload.src_key} → {target_bucket}/{target_key}")

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = replicate_object(
                s3_adapter,
                gcs_adapter,
                payload.src_bucket,
                payload.src_key,
                target_bucket,
                target_key,
            )
            return ReplicationResponse(
                source=ReplicationResult(bucket=payload.src_bucket, key=payload.src_key, status="exists"),
                destination=ReplicationResult(bucket=target_bucket, key=target_key, status="uploaded"),
                result=ReplicationResult(bucket=target_bucket, key=target_key, status="uploaded", size=result.get("size"))
            )
        except Exception as e:
            last_exc = e
            logger.warning(f"Attempt {attempt} failed: {e}")
            time.sleep(RETRY_DELAY)

    logger.exception("Replication failed after retries")
    raise HTTPException(status_code=500, detail=f"Replication failed after {MAX_RETRIES} retries: {last_exc}")

@app.delete("/v1/object/{bucket}/{key}", response_model=DeleteResponse)
def delete_object(bucket: str, key: str, gcs_adapter: LocalGCSAdapter = Depends(get_gcs_adapter)):
    """Delete an object from GCS-like storage."""
    try:
        res = gcs_adapter.delete(bucket, key)
        return DeleteResponse(**res)
    except Exception as e:
        logger.exception("Deletion failed")
        raise HTTPException(status_code=500, detail=f"Deletion failed: {str(e)}")

@app.get("/health", response_model=HealthResponse)
def health_check():
    return {"status": "healthy"}

@app.get("/", response_model=RootResponse)
def root():
    return {"message": "Welcome to the Cross-cloud Replicator API"}
