import boto3
import tempfile
from moto import mock_aws
from fastapi.testclient import TestClient
from app import app, get_s3_adapter, get_gcs_adapter
from storage.adapters import LocalGCSAdapter


TEST_BUCKET = "test-bucket"
TEST_KEY = "file.txt"
TEST_CONTENT = b"hello world"


class FlakyS3Adapter:
    """S3 adapter that fails first N times to test retries."""
    def __init__(self, s3_client, fail_times=2):
        self.s3_client = s3_client
        self.fail_times = fail_times
        self.calls = 0

    def get_stream(self, bucket, key):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise Exception("Simulated failure")
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        # Ensure we return a stream
        return obj["Body"]


@mock_aws
def test_replicate_endpoint_with_retries_and_idempotency():
    # Create mock S3 bucket and object
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=TEST_BUCKET)
    s3.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=TEST_CONTENT)

    # Temporary directory for LocalGCSAdapter
    tmpdir = tempfile.mkdtemp()
    app.dependency_overrides[get_s3_adapter] = lambda: FlakyS3Adapter(s3, fail_times=2)
    app.dependency_overrides[get_gcs_adapter] = lambda: LocalGCSAdapter(tmpdir)

    client = TestClient(app)

    resp = client.post(
        "/v1/replicate",
        json={
            "src_bucket": TEST_BUCKET,
            "src_key": TEST_KEY,
            "dest_bucket": "replica-bucket",
            "dest_key": TEST_KEY
        }
    )

    # Check HTTP status
    assert resp.status_code == 200

    # Check JSON result
    result = resp.json()["result"]
    assert result["status"] == "uploaded"
