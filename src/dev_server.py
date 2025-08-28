import os
import logging
from typing import Optional, cast
import boto3
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from moto import mock_aws

from src.storage.adapters import S3Adapter, LocalGCSAdapter
from .replicator import replicate_object

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------
# FastAPI App
# ----------------------------
app = FastAPI(title="Cross-cloud Replicator (dev mode)")

# ----------------------------
# Globals for singleton mocks
# ----------------------------
MOCK_S3: Optional[S3Adapter] = None
MOCK_GCS: Optional[LocalGCSAdapter] = None
MOCK_BUCKET = "source-bucket"
MOCK_GCS_PATH = os.path.abspath("./mock_gcs_storage")

# ----------------------------
# Dependency injection
# ----------------------------
def get_mock_s3() -> S3Adapter:
    assert MOCK_S3 is not None, "MOCK_S3 not initialized yet"
    return cast(S3Adapter, MOCK_S3)

def get_mock_gcs() -> LocalGCSAdapter:
    assert MOCK_GCS is not None, "MOCK_GCS not initialized yet"
    return cast(LocalGCSAdapter, MOCK_GCS)

# ----------------------------
# Request / Response Models
# ----------------------------
class ReplicationRequest(BaseModel):
    src_bucket: str
    src_key: str
    dest_bucket: str
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

class RootAPIResponse(BaseModel):  # renamed to avoid redeclaration
    message: str

# ----------------------------
# Startup / Shutdown
# ----------------------------
@app.on_event("startup")
def startup_mock():
    global MOCK_S3, MOCK_GCS

    # Start Moto S3 mock
    mock = mock_aws()
    mock.start()
    logger.info("Moto S3 mock started")

    # Create S3 client and pre-load bucket/object
    s3_client = boto3.client("s3", region_name="us-east-1")
    try:
        s3_client.create_bucket(Bucket=MOCK_BUCKET)
    except Exception:
        pass
    s3_client.put_object(Bucket=MOCK_BUCKET, Key="hello.txt", Body=b"Hello from mock S3!")
    logger.info(f"Preloaded S3://{MOCK_BUCKET}/hello.txt")

    MOCK_S3 = S3Adapter(s3_client)
    MOCK_GCS = LocalGCSAdapter(MOCK_GCS_PATH)
    logger.info(f"Mock GCS storage path: {MOCK_GCS_PATH}")

@app.on_event("shutdown")
def shutdown_mock():
    logger.info("Dev mock server shutdown (mock GCS not cleared)")

# ----------------------------
# Routes
# ----------------------------
@app.get("/", response_model=RootAPIResponse)
def root():
    return {"message": "Cross-cloud Replicator API is running 🚀 (dev mode)"}

@app.get("/health", response_model=HealthResponse)
def health_check():
    return {"status": "ok"}

@app.get("/v1/object/{bucket}/{key}")
def check_object(bucket: str, key: str, gcs: LocalGCSAdapter = Depends(get_mock_gcs)):
    exists = gcs.exists(bucket, key)
    return {"exists": exists, "bucket": bucket, "key": key}

@app.delete("/v1/object/{bucket}/{key}", response_model=DeleteResponse)
def delete_object(bucket: str, key: str, gcs: LocalGCSAdapter = Depends(get_mock_gcs)):
    if not gcs.exists(bucket, key):
        raise HTTPException(status_code=404, detail="Object not found")
    gcs.delete(bucket, key)
    return {"status": "deleted", "bucket": bucket, "key": key}

@app.post("/v1/replicate", response_model=ReplicationResponse)
def replicate(
    req: ReplicationRequest,
    s3: S3Adapter = Depends(get_mock_s3),
    gcs: LocalGCSAdapter = Depends(get_mock_gcs)
):
    target_key = req.dest_key or req.src_key
    try:
        result_dict = replicate_object(s3, gcs, req.src_bucket, req.src_key, req.dest_bucket, target_key)
        logger.info("Replication successful: %s/%s -> %s/%s", req.src_bucket, req.src_key, req.dest_bucket, target_key)

        return ReplicationResponse(
            source=ReplicationResult(bucket=req.src_bucket, key=req.src_key, status="exists"),
            destination=ReplicationResult(bucket=req.dest_bucket, key=target_key, status="uploaded"),
            result=ReplicationResult(bucket=req.dest_bucket, key=target_key, status="uploaded", size=result_dict.get("size"))
        )
    except Exception as e:
        logger.exception("Replication failed")
        raise HTTPException(status_code=500, detail=str(e))
