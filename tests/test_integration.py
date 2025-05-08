"""
Integration tests for the upload service.
"""
import json
import time
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch

from upload_service.models import UploadRequest, UploadResult
from upload_service.coordinator import UploadCoordinator

def test_end_to_end_upload_with_state_persistence(tmp_upload_dir, tmp_log_dir, tmp_state_file):
    """Test end-to-end upload flow with state persistence."""
    # Create test files
    file1 = tmp_upload_dir / "test1.txt"
    file2 = tmp_upload_dir / "test2.txt"
    file1.write_text("content 1")
    file2.write_text("content 2")
    
    # Create coordinator
    coordinator = UploadCoordinator(
        log_dir=tmp_log_dir,
        state_file=tmp_state_file,
        scan_interval=0.1
    )
    
    # Mock S3 uploader
    mock_uploader = MagicMock()
    mock_uploader.upload_files.return_value.results = [
        UploadResult(
            file_path=file1,
            s3_key="test1.txt",
            success=True
        ),
        UploadResult(
            file_path=file2,
            s3_key="test2.txt",
            success=True
        )
    ]
    
    with patch('upload_service.coordinator.S3Uploader', return_value=mock_uploader):
        # Start upload
        request = UploadRequest(
            upload_id="test-upload",
            source_folder=tmp_upload_dir,
            destination_bucket="test-bucket",
            pattern="*.txt"
        )
        coordinator.start_upload(request)
        
        # Wait for initial upload
        time.sleep(0.2)
        
        # Verify state is persisted
        with open(tmp_state_file) as f:
            data = json.load(f)
            state = data['upload_states'][0]
            assert state['upload_id'] == "test-upload"
            assert len(state['completed_files']) == 2
            
        # Create new coordinator to test state restoration
        coordinator2 = UploadCoordinator(
            log_dir=tmp_log_dir,
            state_file=tmp_state_file,
            scan_interval=0.1
        )
        
        # Verify state is restored
        state = coordinator2.tracker.get_upload_state("test-upload")
        assert state is not None
        assert len(state.completed_files) == 2
        
        # Create new file
        file3 = tmp_upload_dir / "test3.txt"
        file3.write_text("content 3")
        
        # Wait for detection
        time.sleep(0.2)
        
        # Verify only new file is uploaded
        mock_uploader.upload_files.assert_called_with(
            [file3],
            "test-bucket"
        )

def test_partial_upload_resume(tmp_upload_dir, tmp_log_dir, tmp_state_file):
    """Test resuming a partial upload."""
    # Create test file
    test_file = tmp_upload_dir / "large.txt"
    test_file.write_text("x" * 1024 * 1024)  # 1MB file
    
    # Create initial state with partial upload
    state = {
        'upload_states': [{
            'upload_id': "test-upload",
            'source_folder': str(tmp_upload_dir),
            'destination_bucket': "test-bucket",
            'pattern': "*.txt",
            'completed_files': [],
            'in_progress_files': {
                str(test_file): {
                    'upload_id': "mpu-123",
                    'part_number': 2,
                    'offset': 1024 * 512  # Half uploaded
                }
            },
            'last_modified_times': {}
        }]
    }
    
    with open(tmp_state_file, 'w') as f:
        json.dump(state, f)
    
    # Create coordinator
    coordinator = UploadCoordinator(
        log_dir=tmp_log_dir,
        state_file=tmp_state_file,
        scan_interval=0.1
    )
    
    # Mock S3 uploader
    mock_uploader = MagicMock()
    mock_uploader.upload_files.return_value.results = [
        UploadResult(
            file_path=test_file,
            s3_key="large.txt",
            success=True,
            multipart_upload_id="mpu-123",
            part_number=2,
            offset=1024 * 512
        )
    ]
    
    with patch('upload_service.coordinator.S3Uploader', return_value=mock_uploader):
        # Let coordinator resume upload
        time.sleep(0.2)
        
        # Verify file was uploaded
        mock_uploader.upload_files.assert_called_with(
            [test_file],
            "test-bucket"
        )
        
        # Verify state is updated
        state = coordinator.tracker.get_upload_state("test-upload")
        assert str(test_file) in state.completed_files
        assert str(test_file) not in state.in_progress_files

def test_cli_registers_and_triggers_monitoring(tmp_upload_dir, tmp_log_dir, tmp_state_file):
    """Test CLI monitoring functionality."""
    from upload_service.cli import handle_start, handle_stop
    import argparse
    
    # Create test file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Mock arguments
    args = argparse.Namespace(
        source_folder=str(tmp_upload_dir),
        bucket="test-bucket",
        upload_id="test-upload",
        pattern="*.txt",
        name=None,
        type=None,
        description=None,
        config=None,
        verbose=False
    )
    
    # Mock S3 uploader
    mock_uploader = MagicMock()
    mock_uploader.upload_files.return_value.results = [
        UploadResult(
            file_path=test_file,
            s3_key="test.txt",
            success=True
        )
    ]
    
    with patch('upload_service.coordinator.S3Uploader', return_value=mock_uploader):
        # Start upload
        with pytest.raises(SystemExit):  # CLI will wait for interrupt
            handle_start(args)
            
            # Wait for initial upload
            time.sleep(0.2)
            
            # Create new file
            new_file = tmp_upload_dir / "new.txt"
            new_file.write_text("new content")
            
            # Wait for detection
            time.sleep(0.2)
            
            # Stop upload
            handle_stop(args)
        
        # Verify both files were uploaded
        assert mock_uploader.upload_files.call_count == 2

def test_file_deleted_before_upload_starts(tmp_upload_dir, tmp_log_dir, tmp_state_file):
    """Test handling of files deleted before upload starts."""
    # Create and immediately delete test file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    test_file.unlink()
    
    coordinator = UploadCoordinator(
        log_dir=tmp_log_dir,
        state_file=tmp_state_file,
        scan_interval=0.1
    )
    
    # Mock S3 uploader
    mock_uploader = MagicMock()
    
    with patch('upload_service.coordinator.S3Uploader', return_value=mock_uploader):
        request = UploadRequest(
            upload_id="test-upload",
            source_folder=tmp_upload_dir,
            destination_bucket="test-bucket",
            pattern="*.txt"
        )
        coordinator.start_upload(request)
        
        # Wait for potential upload
        time.sleep(0.2)
        
        # Verify no upload was attempted
        mock_uploader.upload_files.assert_not_called() 