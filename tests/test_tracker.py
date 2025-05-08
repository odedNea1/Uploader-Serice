"""
Tests for the upload tracker component.
"""
import json
import time
from pathlib import Path
import pytest
from upload_service.models import UploadResult
from upload_service.tracker import UploadTracker

def test_register_upload_creates_new_state(upload_tracker, upload_request):
    """Test that registering an upload creates and persists state."""
    upload_tracker.register_upload(upload_request)
    
    # Check state is created
    state = upload_tracker.get_upload_state(upload_request.upload_id)
    assert state is not None
    assert state.upload_id == upload_request.upload_id
    assert state.source_folder == str(upload_request.source_folder)
    assert state.destination_bucket == upload_request.destination_bucket
    assert state.pattern == upload_request.pattern
    assert len(state.completed_files) == 0
    assert len(state.in_progress_files) == 0
    
    # Check state is persisted
    with open(upload_tracker.state_file) as f:
        data = json.load(f)
        assert len(data['upload_states']) == 1
        assert data['upload_states'][0]['upload_id'] == upload_request.upload_id

def test_mark_file_complete_updates_state(upload_tracker, upload_request, tmp_upload_dir):
    """Test that marking a file as complete updates the state correctly."""
    upload_tracker.register_upload(upload_request)
    
    test_file = tmp_upload_dir / "test.txt"
    mtime = test_file.stat().st_mtime
    
    result = UploadResult(
        file_path=test_file,
        s3_key="test.txt",
        success=True,
        size_bytes=len("test content")
    )
    
    upload_tracker.mark_file_complete(upload_request.upload_id, str(test_file), result)
    
    state = upload_tracker.get_upload_state(upload_request.upload_id)
    assert str(test_file) in state.completed_files
    assert str(test_file) in state.last_modified_times
    assert state.last_modified_times[str(test_file)] == mtime

def test_register_multipart_upload_tracks_in_progress(upload_tracker, upload_request, tmp_upload_dir):
    """Test that registering a multipart upload tracks in-progress state."""
    upload_tracker.register_upload(upload_request)
    
    test_file = tmp_upload_dir / "test.txt"
    upload_tracker.register_multipart_upload(
        upload_request.upload_id,
        str(test_file),
        "mpu-123",
        1,
        1024
    )
    
    state = upload_tracker.get_upload_state(upload_request.upload_id)
    assert str(test_file) in state.in_progress_files
    assert state.in_progress_files[str(test_file)]['upload_id'] == "mpu-123"
    assert state.in_progress_files[str(test_file)]['part_number'] == 1
    assert state.in_progress_files[str(test_file)]['offset'] == 1024

def test_get_incomplete_files_returns_expected_set(upload_tracker, upload_request, tmp_upload_dir):
    """Test that get_incomplete_files returns the correct set of files."""
    upload_tracker.register_upload(upload_request)
    
    # Create test files
    completed_file = tmp_upload_dir / "completed.txt"
    completed_file.write_text("completed")
    modified_file = tmp_upload_dir / "modified.txt"
    modified_file.write_text("modified")
    new_file = tmp_upload_dir / "new.txt"
    new_file.write_text("new")
    
    # Mark one file as completed
    result = UploadResult(
        file_path=completed_file,
        s3_key="completed.txt",
        success=True
    )
    upload_tracker.mark_file_complete(upload_request.upload_id, str(completed_file), result)
    
    # Modify a completed file
    time.sleep(0.1)  # Ensure mtime changes
    modified_file.write_text("modified again")
    
    incomplete = upload_tracker.get_incomplete_files(upload_request.upload_id)
    assert str(new_file) in incomplete
    assert str(modified_file) in incomplete
    assert str(completed_file) not in incomplete

def test_state_is_restored_from_file_on_init(prepopulated_state_file, tmp_log_dir, tmp_state_file):
    """Test that state is correctly restored from file on initialization."""
    tracker = UploadTracker(log_dir=tmp_log_dir, state_file=tmp_state_file)
    
    state = tracker.get_upload_state(prepopulated_state_file.upload_id)
    assert state is not None
    assert state.upload_id == prepopulated_state_file.upload_id
    assert state.completed_files == prepopulated_state_file.completed_files
    assert state.in_progress_files == prepopulated_state_file.in_progress_files
    assert state.last_modified_times == prepopulated_state_file.last_modified_times

def test_tracker_handles_corrupt_state_file(tmp_log_dir, tmp_state_file):
    """Test that tracker handles corrupt state file gracefully."""
    # Write invalid JSON
    tmp_state_file.write_text("invalid json{")
    
    tracker = UploadTracker(log_dir=tmp_log_dir, state_file=tmp_state_file)
    assert len(tracker._upload_states) == 0 