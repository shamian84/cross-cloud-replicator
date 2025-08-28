import io
import re
import pytest
from replicator import replicate_object


# ---------- Dummy Adapters for Testing ----------

class DummyS3Adapter:
    def __init__(self, objects=None):
        self.objects = objects or {}

    def get_stream(self, bucket, key):
        if key not in self.objects:
            raise KeyError(f"Object {key} not found in bucket {bucket}")
        return io.BytesIO(self.objects[key])


class DummyGCSAdapter:
    def __init__(self):
        self.objects = {}

    def exists(self, bucket, key):
        return key in self.objects

    def upload_stream(self, bucket, key, stream):
        data = stream.read()
        self.objects[key] = data
        return {"size": len(data)}

    def delete(self, bucket, key):
        return self.objects.pop(key, None)


# ---------- Test Cases ----------

def test_successful_replicate(caplog):
    s3 = DummyS3Adapter(objects={"file1.txt": b"hello world"})
    gcs = DummyGCSAdapter()

    result = replicate_object(s3, gcs, "src", "file1.txt", "dest")
    assert result["status"] == "uploaded"
    assert "file1.txt" in gcs.objects
    assert gcs.objects["file1.txt"] == b"hello world"

    log_msg = " ".join(caplog.messages)
    assert re.search(r"\[Request [0-9a-f\-]{36}\]", log_msg)


def test_missing_source_object(caplog):
    s3 = DummyS3Adapter(objects={})
    gcs = DummyGCSAdapter()

    with pytest.raises(KeyError):
        replicate_object(s3, gcs, "src", "missing.txt", "dest")

    log_msg = " ".join(caplog.messages)
    assert "downloading from s3" in log_msg.lower()
    assert re.search(r"\[Request [0-9a-f\-]{36}\]", log_msg)


def test_delete_twice(caplog):
    gcs = DummyGCSAdapter()
    gcs.objects["file1.txt"] = b"hello"

    deleted = gcs.delete("dest", "file1.txt")
    assert deleted == b"hello"

    deleted = gcs.delete("dest", "file1.txt")
    assert deleted is None


def test_large_file_replication(caplog):
    large_data = b"x" * (10**6 * 5)
    s3 = DummyS3Adapter(objects={"big.bin": large_data})
    gcs = DummyGCSAdapter()

    result = replicate_object(s3, gcs, "src", "big.bin", "dest")
    assert result["status"] == "uploaded"
    assert "big.bin" in gcs.objects
    assert len(gcs.objects["big.bin"]) == len(large_data)

    log_msg = " ".join(caplog.messages)
    assert "Replicated successfully" in log_msg
    assert re.search(r"\[Request [0-9a-f\-]{36}\]", log_msg)


def test_idempotent_replication(caplog):
    s3 = DummyS3Adapter(objects={"file1.txt": b"hello world"})
    gcs = DummyGCSAdapter()

    # First replication → uploaded
    result1 = replicate_object(s3, gcs, "src", "file1.txt", "dest")
    assert result1["status"] == "uploaded"

    # Second replication → skipped (idempotent)
    result2 = replicate_object(s3, gcs, "src", "file1.txt", "dest")
    assert result2["status"] in ("uploaded", "skipped")

    # Ensure no duplicate objects
    assert len(gcs.objects) == 1
    assert gcs.objects["file1.txt"] == b"hello world"

    log_msg = " ".join(caplog.messages)
    assert re.search(r"\[Request [0-9a-f\-]{36}\]", log_msg)
    assert log_msg.count("Replicated successfully") == 1