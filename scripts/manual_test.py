# scripts/manual_test.py
import sys
import os
import tempfile
import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

# --- Ensure src folder is on sys.path ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from app import app, get_s3_adapter, get_gcs_adapter
from storage.adapters import S3Adapter, LocalGCSAdapter


@mock_aws
def run():
    # ---- Setup moto S3 ----
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="my-bucket")
    s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"hello from manual test")

    # ---- Temporary local "GCS" directory ----
    tmpdir = tempfile.mkdtemp()

    # ---- Dependency overrides ----
    app.dependency_overrides[get_s3_adapter] = lambda: S3Adapter(s3)
    app.dependency_overrides[get_gcs_adapter] = lambda: LocalGCSAdapter(tmpdir)

    client = TestClient(app)

    # ---- Call FastAPI replicate endpoint ----
    payload = {
        "s3_bucket": "my-bucket",
        "s3_key": "hello.txt",
        "gcs_bucket": "replica-bucket",
        "gcs_key": "hello.txt"
    }
    resp = client.post("/v1/replicate", json=payload)
    print("\n--- Replication ---")
    print("Status code:", resp.status_code)
    print("Response JSON:", resp.json())

    # ---- Inspect replicated file on disk ----
    dest = os.path.join(tmpdir, "replica-bucket", "hello.txt")
    print("File exists after replicate:", os.path.exists(dest))
    if os.path.exists(dest):
        with open(dest, "rb") as f:
            print("Contents:", f.read())

    # ---- Call FastAPI delete endpoint ----
    delete_resp = client.delete(f"/v1/object/replica-bucket/hello.txt")
    print("\n--- Deletion ---")
    print("Status code:", delete_resp.status_code)
    print("Response JSON:", delete_resp.json())

    # ---- Check file removed ----
    print("File exists after delete:", os.path.exists(dest))


if __name__ == "__main__":
    run()
