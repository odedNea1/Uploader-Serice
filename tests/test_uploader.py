"""
Tests for the S3 uploader component.
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from tenacity import wait_none

from upload_service.uploader import S3Uploader, is_retryable_error

# Patch wait strategy to speed up tests
@pytest.fixture(autouse=True)
def no_wait():
    """Remove wait time between retries for testing."""
    with patch('upload_service.uploader.wait_exponential', return_value=wait_none()):
        yield

def test_upload_file_retries_on_failure(s3_uploader, tmp_upload_dir):
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
    s3_uploader.s3_client = mock_client
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "test.txt")
    
    assert result.success
    assert mock_client.upload_file.call_count == 3

def test_upload_file_returns_correct_result_structure(s3_uploader, tmp_upload_dir):
    """Test that upload returns correct result structure."""
    test_file = tmp_upload_dir / "test.txt"
    content = "test content"
    test_file.write_text(content)
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "test.txt")
    
    assert result.success
    assert result.file_path == test_file
    assert result.s3_key == "test.txt"
    assert result.size_bytes == len(content)
    assert result.error is None

def test_multipart_upload_handles_large_files(s3_uploader, tmp_upload_dir):
    """Test that large files are handled with multipart upload."""
    test_file = tmp_upload_dir / "large.txt"
    content = "x" * (s3_uploader.chunk_size + 1)  # Ensure multipart is used
    test_file.write_text(content)
    
    # Mock multipart upload responses
    mock_client = MagicMock()
    mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_client.upload_part.return_value = {'ETag': 'test-etag'}
    s3_uploader.s3_client = mock_client
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "large.txt")
    
    assert result.success
    assert mock_client.create_multipart_upload.called
    assert mock_client.upload_part.called
    assert mock_client.complete_multipart_upload.called

def test_upload_fails_if_s3_down(s3_uploader, tmp_upload_dir):
    """Test that upload fails gracefully if S3 is down."""
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Mock permanent S3 failure
    error_response = {
        'Error': {
            'Code': 'ServiceUnavailable',
            'Message': 'Service is down'
        }
    }
    error = ClientError(error_response, 'upload_file')
    
    mock_client = MagicMock()
    mock_client.upload_file.side_effect = error
    s3_uploader.s3_client = mock_client
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "test.txt")
    
    assert not result.success
    assert "Service is down" in result.error
    assert mock_client.upload_file.call_count == 3  # Retried max times

def test_concurrent_uploads_handle_errors(s3_uploader, tmp_upload_dir):
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
    s3_uploader.s3_client = mock_client
    
    summary = s3_uploader.upload_files(files, "test-bucket")
    
    assert summary.total_files == 3
    assert summary.successful_uploads == 2
    assert summary.failed_uploads == 1

def test_is_retryable_error():
    """Test error classification for retries."""
    retryable_codes = [
        'RequestTimeout',
        'RequestTimeoutException',
        'PriorRequestNotComplete',
        'ConnectionError',
        'ThrottlingException',
        'ThrottledException',
        'ServiceUnavailable',
        'Throttling',
        '5XX'
    ]
    
    non_retryable_codes = [
        'AccessDenied',
        'NoSuchBucket',
        'InvalidRequest'
    ]
    
    for code in retryable_codes:
        error = ClientError(
            {
                'Error': {
                    'Code': code,
                    'Message': 'Test error'
                }
            },
            'test_operation'
        )
        assert is_retryable_error(error)
        
    for code in non_retryable_codes:
        error = ClientError(
            {
                'Error': {
                    'Code': code,
                    'Message': 'Test error'
                }
            },
            'test_operation'
        )
        assert not is_retryable_error(error)

def test_multipart_upload_aborts_on_failure(s3_uploader, tmp_upload_dir):
    """Test that multipart upload is aborted on failure."""
    test_file = tmp_upload_dir / "large.txt"
    content = "x" * (s3_uploader.chunk_size + 1)
    test_file.write_text(content)
    
    # Mock client to fail during part upload
    mock_client = MagicMock()
    mock_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_client.upload_part.side_effect = ClientError(
        {
            'Error': {
                'Code': 'InternalError',
                'Message': 'Internal error'
            }
        },
        'upload_part'
    )
    s3_uploader.s3_client = mock_client
    
    result = s3_uploader._upload_file(test_file, "test-bucket", "large.txt")
    
    assert not result.success
    assert "Internal error" in result.error
    assert mock_client.abort_multipart_upload.called 