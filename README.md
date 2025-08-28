# cross-cloud-replicator

[![Python](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.101.0-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey.svg)](LICENSE)

---

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Features](#features)  
3. [Architecture](#architecture)  
4. [Requirements](#requirements)  
5. [Setup & Installation](#setup--installation)  
6. [Running the Application](#running-the-application)  
7. [API Endpoints](#api-endpoints)  
8. [Testing](#testing)  
9. [Folder Structure](#folder-structure)  
10. [Notes & Considerations](#notes--considerations)  
11. [License](#license)  

---

## Features

1. Replicate objects between S3 and GCS-like storage.
2. Retry mechanism for transient failures.
3. Delete objects from GCS-like storage.
4. Health check endpoint.
5. Dev mode using Moto to mock AWS S3.
6. Local storage adapter for fast testing without cloud dependencies.

---

## Architecture

+----------------+       +------------------+       +--------------------+
| FastAPI Server | <-->  |  S3 Adapter      | <-->  | Amazon S3          |
| /v1/replicate  |       | (Boto3 Wrapper)  |       | Bucket/Object      |
+----------------+       +------------------+       +--------------------+
        |
        v
+--------------------+
| Local GCS Adapter  |
| (filesystem-based) |
+--------------------+

--> Adapters: Abstract away cloud provider details (S3, GCS).
--> Replicator function: Handles data streaming, retries, and logging.
--> Dev mode: Uses Moto to mock S3 and temporary directories for GCS.


---

## Folder Structure

cross-cloud-replicator/
├── src/
│   ├── dev_server.py          # Dev-mode with mock S3/GCS [attached_image:1]
│   ├── replicator.py          # Replication logic [attached_image:1]
│   └── storage/
│       └── adapters.py        # S3Adapter, LocalGCSAdapter [attached_image:1]
├── tests/
│   ├── conftest.py
│   ├── test_replicator_basic.py
│   ├── test_replicator_retries.py
│   ├── test_replicator_scalability.py
│   └── test_replication_cases.py
├── mock_storage/
│   └── replica-bucket.py/
|
├── app.py                     # Main FastAPI app
├── .gitignore
├── LICENSE
├── pytest.ini
├── README.md
├── requirements.txt
├── scripts/

---

## Setup & Installation

1. Clone the repository:

>> git clone <repository-url>
>> cd cross-cloud-replicator

2. Create and activate a Python virtual environment:

>> python -m venv .venv
# Windows
>> .venv\Scripts\activate
# Linux/Mac
>> source .venv/bin/activate

3. Install dependencies:

>> pip install --upgrade pip
>> pip install -r requirements.txt

---

## Configuration

| Variable            | Default          | Description                     |
| ------------------- | ---------------- | ------------------------------- |
| `LOCAL_GCS_PATH`    | `/tmp/local_gcs` | Path for local GCS storage      |
| `TARGET_GCS_BUCKET` | `replica-bucket` | Default destination bucket      |
| `MAX_RETRIES`       | `3`              | Maximum retry attempts          |
| `RETRY_DELAY`       | `0.1`            | Delay between retries (seconds) |


---

## Usage

Run API in Local Mode : 

>> uvicorn src.app:app --reload

>> Access API at http://127.0.0.1:8000
>> Swagger docs at http://127.0.0.1:8000/docs

Run API in Dev Mode (Moto Mock S3) :

>> uvicorn src.dev_server:app --reload

>> Preloaded mock bucket: source-bucket
>> Preloaded object: hello.txt


---

## API Endpoints

| Endpoint                    | Method | Description                    |
| --------------------------- | ------ | ------------------------------ |
| `/v1/replicate`             | POST   | Replicate object from S3 → GCS |
| `/v1/object/{bucket}/{key}` | DELETE | Delete object from GCS         |
| `/v1/object/{bucket}/{key}` | GET    | Check object existence in GCS  |
| `/health`                   | GET    | Health check                   |
| `/`                         | GET    | Root message                   |


## Sample Request (POST /v1/replicate)

{
  "src_bucket": "source-bucket",
  "src_key": "hello.txt",
  "dest_bucket": "replica-bucket",
  "dest_key": "hello.txt"
}

---

## Testing

Run all tests using pytest:

>> pytest -v --capture=tee-sys

>> Fixtures use Moto for S3 mocking.
>> Temporary directories are used for GCS-like storage.
>> Dependency overrides ensure FastAPI endpoints use mocked adapters.

---

## Design Decisions

>> Adapters pattern: Makes it easy to add new cloud providers.
>> Streaming data in chunks: Efficient memory usage.
>> Retries with delay: Handles transient network or cloud failures.
>> Dev mode with Moto: Enables testing without AWS account.
>> FastAPI: Async-ready, automatic Swagger docs, lightweight.


---

## Future Improvements

>> Add Azure Blob / real GCS adapter.
>> Support multi-object batch replication.
>> Add authentication/authorization for API endpoints.
>> Add logging to external systems (CloudWatch, ELK).
>> Add unit tests for failure scenarios and edge cases.


---

## License

>> This project is licensed under the MIT License – see the LICENSE file for details.