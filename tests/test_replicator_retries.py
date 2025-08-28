import io
import pytest
from unittest.mock import MagicMock
import botocore.exceptions as boto_exceptions
from replicator import replicate_object, MAX_RETRIES


def test_retry_and_success():
    s3 = MagicMock()
    gcs = MagicMock()

    s3.get_stream.side_effect = [
        boto_exceptions.EndpointConnectionError(endpoint_url="mock"),
        boto_exceptions.EndpointConnectionError(endpoint_url="mock"),
        io.BytesIO(b"data-stream"),
    ]
    gcs.exists.return_value = False
    gcs.upload_stream.return_value = {"etag": "mock"}

    result = replicate_object(s3, gcs, "src-bucket", "obj.txt", "dest-bucket")
    assert result["status"] == "uploaded"
    assert s3.get_stream.call_count == 3
    assert gcs.upload_stream.called


def test_fail_after_max_retries():
    s3 = MagicMock()
    gcs = MagicMock()
    s3.get_stream.side_effect = boto_exceptions.EndpointConnectionError(endpoint_url="mock")
    gcs.exists.return_value = False

    import pytest
    with pytest.raises(boto_exceptions.EndpointConnectionError):
        replicate_object(s3, gcs, "src-bucket", "obj.txt", "dest-bucket")

    assert s3.get_stream.call_count == MAX_RETRIES
    assert not gcs.upload_stream.called


def test_skip_if_exists():
    s3 = MagicMock()
    gcs = MagicMock()
    gcs.exists.return_value = True

    result = replicate_object(s3, gcs, "src-bucket", "obj.txt", "dest-bucket")
    assert result["status"] == "skipped"
    assert not s3.get_stream.called
    assert not gcs.upload_stream.called


def test_request_id_preserved():
    s3 = MagicMock()
    gcs = MagicMock()
    gcs.exists.return_value = False
    s3.get_stream.return_value = io.BytesIO(b"data")
    gcs.upload_stream.return_value = {"etag": "mock"}

    context = {"request_id": "test-123"}
    result = replicate_object(s3, gcs, "src-bucket", "obj.txt", "dest-bucket", context=context)
    assert result["request_id"] == "test-123"
    assert result["status"] == "uploaded"


def test_retry_logs_are_emitted(caplog):
    s3 = MagicMock()
    gcs = MagicMock()
    gcs.exists.return_value = False
    s3.get_stream.side_effect = [
        boto_exceptions.EndpointConnectionError(endpoint_url="mock"),
        boto_exceptions.EndpointConnectionError(endpoint_url="mock"),
        io.BytesIO(b"data-stream"),
    ]
    gcs.upload_stream.return_value = {"etag": "mock"}

    result = replicate_object(s3, gcs, "src-bucket", "obj.txt", "dest-bucket")
    assert result["status"] == "uploaded"

    retry_logs = [rec.message for rec in caplog.records if "Retrying" in rec.message]
    success_logs = [rec.message for rec in caplog.records if "Replicated successfully" in rec.message]
    assert retry_logs
    assert success_logs
