"""
Test fixtures for the upload service.
"""
import json
import os
import uuid
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws as moto_mock_aws

from upload_service.models import UploadRequest, UploadState
from upload_service.tracker import UploadTracker
from upload_service.uploader import S3Uploader
from upload_service.coordinator import UploadCoordinator

@pytest.fixture
def tmp_upload_dir(tmp_path):
    """Create a temporary directory for test files."""
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    return upload_dir

@pytest.fixture
def tmp_log_dir(tmp_path):
    """Create a temporary directory for logs."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return log_dir

@pytest.fixture
def tmp_state_file(tmp_path):
    """Create a temporary state file."""
    return tmp_path / "upload_state.json"

@pytest.fixture
def upload_request(tmp_upload_dir):
    """Create a test upload request."""
    # Create a test file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    return UploadRequest(
        upload_id=str(uuid.uuid4()),
        source_folder=tmp_upload_dir,
        destination_bucket="test-bucket",
        pattern="*.txt",
        name="Test Upload",
        type="test",
        description="Test upload request"
    )

@pytest.fixture
def mock_aws():
    """Mock S3 client using moto."""
    with moto_mock_aws():
        s3 = boto3.client('s3')
        # Create test bucket
        s3.create_bucket(Bucket='test-bucket')
        yield s3

@pytest.fixture
def upload_tracker(tmp_log_dir, tmp_state_file):
    """Create a test upload tracker."""
    return UploadTracker(log_dir=tmp_log_dir, state_file=tmp_state_file)

@pytest.fixture
def s3_uploader(mock_aws):
    """Create a test S3 uploader."""
    return S3Uploader()

@pytest.fixture
def upload_coordinator(tmp_log_dir, tmp_state_file):
    """Create a test upload coordinator."""
    return UploadCoordinator(
        log_dir=tmp_log_dir,
        state_file=tmp_state_file,
        scan_interval=1  # Fast scanning for tests
    )

@pytest.fixture
def prepopulated_state_file(tmp_state_file, tmp_upload_dir):
    """Create a state file with test data."""
    state = UploadState(
        upload_id="test-upload",
        source_folder=str(tmp_upload_dir),
        destination_bucket="test-bucket",
        pattern="*.txt",
        completed_files=["test1.txt"],
        in_progress_files={
            "test2.txt": {
                "upload_id": "mpu-123",
                "part_number": 1,
                "offset": 1024
            }
        },
        last_modified_times={
            "test1.txt": 1234567890.0
        }
    )
    
    data = {
        'upload_states': [
            {
                'upload_id': state.upload_id,
                'source_folder': state.source_folder,
                'destination_bucket': state.destination_bucket,
                'pattern': state.pattern,
                'completed_files': state.completed_files,
                'in_progress_files': state.in_progress_files,
                'last_modified_times': state.last_modified_times
            }
        ]
    }
    
    with open(tmp_state_file, 'w') as f:
        json.dump(data, f, indent=2)
        
    return state 