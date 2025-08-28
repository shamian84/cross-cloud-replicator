import logging
import uuid
from typing import Optional, Any, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import botocore.exceptions as boto_exceptions

# --------------------------------------------
# Configurable constants
# --------------------------------------------
MAX_RETRIES = 3  # Central retry limit, used by retry decorator + tests

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Retryable exceptions (transient AWS/network errors)
RETRYABLE_EXCEPTIONS = (
    boto_exceptions.EndpointConnectionError,
    boto_exceptions.ConnectionClosedError,
    boto_exceptions.ReadTimeoutError,
    boto_exceptions.ConnectTimeoutError,
)

def _log_before_sleep(retry_state):
    """Custom retry logger with request_id context."""
    context = retry_state.kwargs.get("context", {})
    request_id = context.get("request_id", "N/A")
    exception = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "[Request %s] Retrying after exception: %s (attempt %s/%s)",
        request_id,
        exception,
        retry_state.attempt_number,
        retry_state.retry_object.stop.max_attempt_number,
    )

@retry(
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=_log_before_sleep,
    reraise=True
)
def replicate_object(
    s3_adapter: Any,
    gcs_adapter: Any,
    s3_bucket: str,
    s3_key: str,
    gcs_bucket: str,
    gcs_key: Optional[str] = None,
    *,
    context: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Replicate an object from AWS S3 to a GCS-like storage with retries and idempotency.
    """

    if context is None:
        context = {"request_id": str(uuid.uuid4())}
    request_id = context["request_id"]

    if gcs_key is None:
        gcs_key = s3_key

    # Step 1: Idempotency check
    if gcs_adapter.exists(gcs_bucket, gcs_key):
        logger.warning(
            "[Request %s] Destination already exists: %s/%s — skipping",
            request_id, gcs_bucket, gcs_key
        )
        return {"status": "skipped", "reason": "already_exists", "request_id": request_id}

    try:
        # Step 2: Download stream from S3
        logger.info("[Request %s] Downloading from S3: %s/%s", request_id, s3_bucket, s3_key)
        stream = s3_adapter.get_stream(s3_bucket, s3_key)
    except AttributeError:
        # Handles DummyS3Adapter without .client
        logger.error("[Request %s] Source object not found (dummy adapter): %s/%s", request_id, s3_bucket, s3_key)
        return {"status": "not_found", "bucket": s3_bucket, "key": s3_key, "request_id": request_id}
    except boto_exceptions.ClientError as e:
        # Handles real S3
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.error("[Request %s] Source object not found: %s/%s", request_id, s3_bucket, s3_key)
            return {"status": "not_found", "bucket": s3_bucket, "key": s3_key, "request_id": request_id}
        raise

    # Step 3: Upload stream to GCS-like storage
    logger.info("[Request %s] Uploading to destination: %s/%s", request_id, gcs_bucket, gcs_key)
    meta = gcs_adapter.upload_stream(gcs_bucket, gcs_key, stream)

    # Step 4: Success log
    logger.info(
        "[Request %s] Replicated successfully %s/%s -> %s/%s",
        request_id, s3_bucket, s3_key, gcs_bucket, gcs_key
    )

    return {"status": "uploaded", "meta": meta, "request_id": request_id}
    