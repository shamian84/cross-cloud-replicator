# tests/conftest.py
import sys
import os
import tempfile
import pytest
import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

# ------------------------
# Ensure src/ is in sys.path
# ------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# ------------------------
# Imports from src
# ------------------------
try:
    from app import app, get_s3_adapter, get_gcs_adapter
except ImportError:
    # fallback if using dev_server
    from dev_server import app, get_mock_s3 as get_s3_adapter, get_mock_gcs as get_gcs_adapter

from storage.adapters import S3Adapter, LocalGCSAdapter

# ------------------------
# Fixture: Moto S3 client
# ------------------------
@pytest.fixture
def mock_s3_client():
    """Provide a moto-mocked S3 client"""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client

# ------------------------
# Fixture: Local GCS adapter (temporary dir)
# ------------------------
@pytest.fixture
def temp_gcs_adapter():
    """Provide LocalGCSAdapter with a temporary directory"""
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = LocalGCSAdapter(tmpdir)
        yield adapter

# ------------------------
# Fixture: S3 adapter wrapper
# ------------------------
@pytest.fixture
def s3_adapter(mock_s3_client):
    """Return S3Adapter for testing"""
    yield S3Adapter(mock_s3_client)

# ------------------------
# Fixture: FastAPI TestClient
# ------------------------
@pytest.fixture
def client(s3_adapter, temp_gcs_adapter):
    """Return FastAPI TestClient with dependency overrides"""
    app.dependency_overrides[get_s3_adapter] = lambda: s3_adapter
    app.dependency_overrides[get_gcs_adapter] = lambda: temp_gcs_adapter
    with TestClient(app) as c:
        yield c
    # Clear overrides after test
    app.dependency_overrides.clear()
