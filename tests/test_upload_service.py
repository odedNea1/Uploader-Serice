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
    from upload_service.scanner import FileScanner
    
    scanner = FileScanner(source_files, "*.txt")
    files = list(scanner.scan_files())
    
    assert len(files) == 3
    assert all(f.suffix == ".txt" for f in files)
    
    # Test relative path calculation
    expected_paths = {"file1.txt", "subdir/file2.txt", "subdir/file3.txt"}
    for file_path in files:
        rel_path = scanner.get_relative_path(file_path)
        # Convert Windows path separators to Unix style for comparison
        normalized_path = str(rel_path).replace("\\", "/")
        assert normalized_path in expected_paths

def test_upload_request_validation(temp_dir):
    nonexistent_dir = temp_dir / "nonexistent"
    with pytest.raises(ValueError):
        UploadRequest(
            upload_id="test",
            source_folder=nonexistent_dir,
            destination_bucket="test-bucket",
            pattern="*.txt",
            name="test",
            type="test",
            description="test"
        )

def test_upload_tracker(temp_dir):
    from upload_service.tracker import UploadTracker
    
    # Create test directory structure
    source_dir = temp_dir / "source"
    source_dir.mkdir()
    log_dir = temp_dir / "logs"
    tracker = UploadTracker(log_dir)
    
    request = UploadRequest(
        upload_id="test-123",
        source_folder=source_dir,
        destination_bucket="test-bucket",
        pattern="*.txt",
        name="test",
        type="test",
        description="test"
    )
    
    tracker.log_upload_request(request)
    
    log_file = next(log_dir.glob("upload_test-123_*.json"))
    log_data = json.loads(log_file.read_text())
    
    assert log_data["upload_id"] == "test-123"
    assert log_data["destination_bucket"] == "test-bucket"

def test_s3_uploader(source_files, mock_s3_client):
    from upload_service.uploader import S3Uploader
    
    uploader = S3Uploader("test-bucket", "test-123")
    
    # Test single file upload
    file_path = next(source_files.glob("*.txt"))
    s3_key = "test-123/file1.txt"
    
    result = uploader._upload_file(file_path, s3_key)
    
    assert result.success
    assert result.file_path == file_path
    assert result.s3_key == s3_key
    assert result.etag == "test-etag"
    mock_s3_client.upload_file.assert_called_once()

def test_upload_coordinator(source_files, mock_s3_client):
    coordinator = UploadCoordinator()
    
    request = UploadRequest(
        upload_id="test-123",
        source_folder=source_files,
        destination_bucket="test-bucket",
        pattern="*.txt",
        name="test",
        type="test",
        description="test"
    )
    
    summary = coordinator.process_upload(request)
    
    assert isinstance(summary, UploadSummary)
    assert summary.upload_id == "test-123"
    assert summary.total_files == 3
    assert summary.successful_uploads == 3
    assert summary.failed_uploads == 0

def test_cli_interface(source_files, mock_s3_client, tmp_path):
    from typer.testing import CliRunner
    from upload_service.cli import app
    
    runner = CliRunner()
    log_dir = tmp_path / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Test successful upload with minimal required arguments
    result = runner.invoke(
        app, 
        [
            str(source_files),
            "test-bucket",
            "--upload-id", "test-123"
        ]
    )
    
    assert result.exit_code == 0, f"CLI failed with: {result.stdout}"
    assert "Upload Summary:" in result.stdout
    assert "Total files: 3" in result.stdout
    assert "Successful uploads: 3" in result.stdout
    assert "Failed uploads: 0" in result.stdout
    
    # Test with all optional arguments
    result = runner.invoke(
        app, 
        [
            str(source_files),
            "test-bucket",
            "--upload-id", "test-456",
            "--pattern", "*.txt",
            "--name", "test",
            "--type", "test",
            "--description", "test",
            "--log-dir", str(log_dir),
            "--max-workers", "3"
        ]
    )
    
    assert result.exit_code == 0, f"CLI failed with: {result.stdout}"
    assert any(log_dir.glob("upload_test-456_*.json"))

    # Test missing required argument (source_folder)
    result = runner.invoke(app, ["upload"])
    assert result.exit_code == 2
    assert "Missing argument" in result.stdout


    # Test invalid source folder
    result = runner.invoke(
        app,
        [
            str(tmp_path / "nonexistent"),
            "test-bucket",
            "--upload-id", "test-789"
        ]
    )
    assert result.exit_code == 1
    assert "Error: Source folder" in result.stdout

    # Test missing required option (upload-id)
    result = runner.invoke(
        app,
        [
            str(source_files),
            "test-bucket"
        ]
    )
    assert result.exit_code == 2
    assert "Missing option" in result.stdout and "--upload-id" in result.stdout
