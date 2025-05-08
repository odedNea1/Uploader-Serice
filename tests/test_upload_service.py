import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from upload_service import UploadCoordinator, UploadRequest
from upload_service.models import UploadResult, UploadSummary

@pytest.fixture
def temp_dir():
    with TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)

@pytest.fixture
def source_files(temp_dir):
    # Create a test directory structure
    test_files = {
        "file1.txt": "Test content 1",
        "subdir/file2.txt": "Test content 2",
        "subdir/file3.txt": "Test content 3",
    }
    
    for rel_path, content in test_files.items():
        file_path = temp_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)
        
    return temp_dir

@pytest.fixture
def mock_s3_client():
    with patch('boto3.client') as mock_boto3:
        mock_client = MagicMock()
        mock_client.head_object.return_value = {'ETag': '"test-etag"'}
        mock_boto3.return_value = mock_client
        yield mock_client

def test_file_scanner(source_files):
    """Test file scanning functionality."""
    from upload_service.scanner import FileScanner
    
    request = UploadRequest(
        upload_id="test-123",
        source_folder=source_files,
        destination_bucket="test-bucket",
        pattern="*.txt"
    )
    
    scanner = FileScanner()
    files = scanner.scan(request)
    
    assert len(files) == 2
    assert all(f.name.endswith('.txt') for f in files)

def test_upload_request_validation():
    """Test upload request validation."""
    with pytest.raises(ValueError):
        UploadRequest(
            upload_id="",  # Empty ID
            source_folder=Path("/nonexistent"),
            destination_bucket="test-bucket",
            pattern="*.txt"
        )
    
    with pytest.raises(ValueError):
        UploadRequest(
            upload_id="test",
            source_folder=Path("/nonexistent"),  # Non-existent folder
            destination_bucket="test-bucket",
            pattern="*.txt"
        )
    
    with pytest.raises(ValueError):
        UploadRequest(
            upload_id="test",
            source_folder=Path.cwd(),
            destination_bucket="",  # Empty bucket
            pattern="*.txt"
        )

def test_upload_tracker(temp_dir):
    """Test upload tracking functionality."""
    from upload_service.tracker import UploadTracker
    
    log_dir = temp_dir / "logs"
    log_dir.mkdir()
    state_file = temp_dir / "state.json"
    
    tracker = UploadTracker(log_dir=log_dir, state_file=state_file)
    
    request = UploadRequest(
        upload_id="test-123",
        source_folder=temp_dir,
        destination_bucket="test-bucket",
        pattern="*.txt"
    )
    
    # Register upload
    tracker.register_upload(request)
    state = tracker.get_upload_state("test-123")
    assert state is not None
    assert state.upload_id == "test-123"

def test_s3_uploader(source_files, mock_s3_client):
    """Test S3 upload functionality."""
    from upload_service.uploader import S3Uploader
    
    uploader = S3Uploader()
    
    # Test single file upload
    file_path = next(source_files.glob("*.txt"))
    result = uploader._upload_file(file_path, "test-bucket", "test.txt")
    
    assert result.success
    assert result.file_path == file_path
    assert result.s3_key == "test.txt"

def test_upload_coordinator(source_files, mock_s3_client, temp_dir):
    """Test upload coordination."""
    from upload_service.coordinator import UploadCoordinator
    
    log_dir = temp_dir / "logs"
    log_dir.mkdir()
    state_file = temp_dir / "state.json"
    
    coordinator = UploadCoordinator(
        log_dir=log_dir,
        state_file=state_file,
        scan_interval=0.1
    )
    
    request = UploadRequest(
        upload_id="test-123",
        source_folder=source_files,
        destination_bucket="test-bucket",
        pattern="*.txt"
    )
    
    coordinator.start_upload(request)
    
    # Wait for initial upload
    import time
    time.sleep(0.2)
    
    # Verify state
    state = coordinator.tracker.get_upload_state("test-123")
    assert state is not None
    assert len(state.completed_files) > 0

def test_cli_interface(source_files, mock_s3_client, temp_dir):
    """Test CLI interface."""
    from upload_service.cli import handle_start, handle_stop
    import argparse
    
    args = argparse.Namespace(
        source_folder=str(source_files),
        bucket="test-bucket",
        upload_id="test-123",
        pattern="*.txt",
        name=None,
        type=None,
        description=None,
        config=None,
        verbose=False,
        log_dir=str(temp_dir / "logs"),
        state_file=str(temp_dir / "state.json")
    )
    
    # Create log directory
    (temp_dir / "logs").mkdir()
    
    with pytest.raises(SystemExit):  # CLI will wait for interrupt
        handle_start(args)
        
        # Wait for initial upload
        import time
        time.sleep(0.2)
        
        handle_stop(args)
