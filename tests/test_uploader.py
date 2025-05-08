"""
Tests for the S3 uploader component.
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from tenacity import wait_none

from upload_service.uploader import S3Uploader

# Patch wait strategy to speed up tests
@pytest.fixture(autouse=True)
def no_wait():
    """Remove wait time between retries for testing."""
    with patch('upload_service.uploader.wait_exponential', return_value=wait_none()):
        yield

def test_upload_file_retries_on_failure(tmp_upload_dir, mock_aws):
    """Test that upload retries on transient failures."""
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Mock S3 client to fail twice then succeed
    error_response = {
        'Error': {
            'Code': 'RequestTimeout',
            'Message': 'Request timed out'
        }
    }
    error = ClientError(error_response, 'upload_file')
    
    mock_client = MagicMock()
    mock_client.upload_file.side_effect = [
        error,
        error,
        None  # Success on third try
    ]
    
    s3_uploader = S3Uploader()
    s3_uploader.s3_client = mock_client
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "test.txt")
    
    assert result.success
    assert mock_client.upload_file.call_count == 3

def test_upload_file_returns_correct_result_structure(tmp_upload_dir, mock_aws):
    """Test that upload returns correct result structure."""
    test_file = tmp_upload_dir / "test.txt"
    content = "test content"
    test_file.write_text(content)
    
    s3_uploader = S3Uploader()
    result = s3_uploader._upload_file(test_file, "test-bucket", "test.txt")
    
    assert result.success
    assert result.file_path == test_file
    assert result.s3_key == "test.txt"
    assert result.size_bytes == len(content)
    assert result.error is None

def test_multipart_upload_handles_large_files(tmp_upload_dir, mock_aws):
    """Test that large files are handled with multipart upload."""
    test_file = tmp_upload_dir / "large.txt"
    content = "x" * (20 * 1024 * 1024)  # 20MB file to ensure multipart is used
    test_file.write_text(content)
    
    s3_uploader = S3Uploader()
    result = s3_uploader._upload_file(test_file, "test-bucket", "large.txt")
    
    assert result.success
    assert result.multipart_upload_id is not None

def test_concurrent_uploads_handle_errors(tmp_upload_dir, mock_aws):
    """Test that concurrent uploads handle individual file failures."""
    # Create test files
    files = []
    for i in range(3):
        file = tmp_upload_dir / f"test{i}.txt"
        file.write_text(f"content {i}")
        files.append(file)
    
    # Mock S3 client to fail for one file
    def mock_upload_file(file, bucket, key, **kwargs):
        if "test1.txt" in str(file):
            raise ClientError(
                {
                    'Error': {
                        'Code': 'AccessDenied',
                        'Message': 'Access denied'
                    }
                },
                'upload_file'
            )
    
    mock_client = MagicMock()
    mock_client.upload_file.side_effect = mock_upload_file
    
    s3_uploader = S3Uploader()
    s3_uploader.s3_client = mock_client
    
    summary = s3_uploader.upload_files(files, "test-bucket")
    
    assert summary.total_files == 3
    assert summary.successful_uploads == 2
    assert summary.failed_uploads == 1
