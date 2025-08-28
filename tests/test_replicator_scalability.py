from moto import mock_aws
import boto3
import tempfile
from fastapi.testclient import TestClient
from app import app, get_s3_adapter, get_gcs_adapter
from storage.adapters import LocalGCSAdapter, S3Adapter

@mock_aws
def test_scalability_in_process():
    # Create mock S3 bucket and objects
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="source-bucket")
    for i in range(10):
        s3.put_object(Bucket="source-bucket", Key=f"file{i}.txt", Body=f"data{i}".encode())

    # Use temporary directory for GCS adapter
    with tempfile.TemporaryDirectory() as tmpdir:
        app.dependency_overrides[get_s3_adapter] = lambda: S3Adapter(s3)
        app.dependency_overrides[get_gcs_adapter] = lambda: LocalGCSAdapter(tmpdir)

        client = TestClient(app)

        # Fixed JSON keys
        for i in range(10):
            resp = client.post(
                "/v1/replicate",
                json={
                    "src_bucket": "source-bucket",
                    "src_key": f"file{i}.txt",
                    "dest_bucket": "replica-bucket",
                    "dest_key": f"file{i}.txt"
                }
            )
            assert resp.status_code == 200
            assert resp.json()["result"]["status"] == "uploaded"
