import os
import tempfile
import logging
from typing import Optional, Dict, Any, BinaryIO
from botocore.exceptions import ClientError
import boto3

logger = logging.getLogger(__name__)
CHUNK_SIZE = 8192

# ----------------------------------------------------------------------
# S3 Adapter
# ----------------------------------------------------------------------
class S3Adapter:
    """Wrapper around boto3 S3 client with safe error handling."""

    def __init__(self, client: boto3.client) -> None: # type: ignore
        self.client = client

    def get_stream(self, bucket: str, key: str) -> BinaryIO:
        """Return a streaming body for an S3 object."""
        try:
            resp = self.client.get_object(Bucket=bucket, Key=key)
            return resp["Body"]
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"{bucket}/{key}") from e
            raise

    def delete(self, bucket: str, key: str) -> Dict[str, Any]:
        """Delete an object from S3."""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return {"bucket": bucket, "key": key, "status": "not_found"}
            raise

        self.client.delete_object(Bucket=bucket, Key=key)
        return {"bucket": bucket, "key": key, "status": "deleted"}

    def list_objects(self, bucket: str) -> Dict[str, Any]:
        """List all objects in a bucket."""
        try:
            resp = self.client.list_objects_v2(Bucket=bucket)
            contents = resp.get("Contents", [])
            objects = [{"key": obj["Key"], "size": obj["Size"]} for obj in contents]
            return {"bucket": bucket, "objects": objects}
        except self.client.exceptions.NoSuchBucket:
            return {"bucket": bucket, "objects": [], "status": "not_found"}


# ----------------------------------------------------------------------
# Local GCS Adapter
# ----------------------------------------------------------------------
class LocalGCSAdapter:
    """Simulate GCS using local filesystem."""

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def _abs_path(self, bucket: str, key: str) -> str:
        safe_key = os.path.normpath(key).replace("\\", "/")
        if safe_key.startswith(".."):
            raise ValueError(f"Unsafe key: {key}")
        return os.path.join(self.base_path, bucket, safe_key)

    def exists(self, bucket: str, key: str) -> bool:
        path = self._abs_path(bucket, key)
        return os.path.exists(path)

    def upload_stream(self, bucket: str, key: str, stream: BinaryIO) -> Dict[str, Any]:
        dest_path = self._abs_path(bucket, key)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        written = 0
        with open(dest_path, "wb") as f:
            while chunk := stream.read(CHUNK_SIZE):
                f.write(chunk)
                written += len(chunk)
        return {"bucket": bucket, "key": key, "size": written}

    def delete(self, bucket: str, key: str) -> Dict[str, Any]:
        path = self._abs_path(bucket, key)
        if os.path.exists(path):
            os.remove(path)
            return {"bucket": bucket, "key": key, "status": "deleted"}
        return {"bucket": bucket, "key": key, "status": "not_found"}

    def list_objects(self, bucket: str) -> Dict[str, Any]:
        bucket_path = os.path.join(self.base_path, bucket)
        if not os.path.exists(bucket_path):
            return {"bucket": bucket, "objects": [], "status": "not_found"}

        objects = []
        for root, _, files in os.walk(bucket_path):
            for f in files:
                rel_path = os.path.relpath(os.path.join(root, f), bucket_path)
                objects.append(
                    {"key": rel_path.replace("\\", "/"), "size": os.path.getsize(os.path.join(root, f))}
                )
        return {"bucket": bucket, "objects": objects}


# ----------------------------------------------------------------------
# Mock factories (Pylance-safe)
# ----------------------------------------------------------------------
def get_mock_s3() -> S3Adapter:
    """
    Return an S3Adapter backed by moto in-memory S3.
    """
    from moto import mock_aws  # local import for typing safety
    mock = mock_aws()
    mock.start()
    client = boto3.client("s3", region_name="us-east-1")  # type: ignore
    return S3Adapter(client)


def get_mock_gcs() -> LocalGCSAdapter:
    """Return a LocalGCSAdapter pointing to a temporary folder."""
    tmpdir = tempfile.mkdtemp(prefix="mock_gcs_")
    return LocalGCSAdapter(tmpdir)
