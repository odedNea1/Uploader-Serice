"""
Tests for the folder monitor component.
"""
import time
from pathlib import Path
import pytest
from unittest.mock import Mock

from upload_service.monitor import FolderMonitor

def test_monitor_detects_new_file_and_triggers_callback(tmp_upload_dir):
    """Test that monitor detects new files and triggers callback."""
    monitor = FolderMonitor(scan_interval=0.1)  # Fast scanning for tests
    callback = Mock()
    monitor.register_callback(callback)
    
    # Register folder to monitor
    monitor.register_folder(
        "test-upload",
        tmp_upload_dir,
        "test-bucket",
        "*.txt"
    )
    
    try:
        # Wait for initial scan
        time.sleep(0.2)
        
        # Create new file
        test_file = tmp_upload_dir / "test.txt"
        test_file.write_text("test content")
        
        # Wait for detection
        time.sleep(0.2)
        
        # Check callback was called with correct args
        callback.assert_called_with("test-upload", {test_file})
        
    finally:
        monitor.stop_all()

def test_monitor_does_not_reupload_unmodified_files(tmp_upload_dir):
    """Test that monitor doesn't trigger callback for unmodified files."""
    monitor = FolderMonitor(scan_interval=0.1)
    callback = Mock()
    monitor.register_callback(callback)
    
    # Create test file before monitoring
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Register folder to monitor
    monitor.register_folder(
        "test-upload",
        tmp_upload_dir,
        "test-bucket",
        "*.txt"
    )
    
    try:
        # Wait for potential callbacks
        time.sleep(0.3)
        
        # After initial detection, callback shouldn't be called again
        callback.assert_called_once()
        
    finally:
        monitor.stop_all()

def test_monitor_uploads_modified_file(tmp_upload_dir):
    """Test that monitor detects modified files."""
    monitor = FolderMonitor(scan_interval=0.1)
    callback = Mock()
    monitor.register_callback(callback)
    
    # Create test file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("initial content")
    
    # Register folder to monitor
    monitor.register_folder(
        "test-upload",
        tmp_upload_dir,
        "test-bucket",
        "*.txt"
    )
    
    try:
        # Wait for initial scan
        time.sleep(0.2)
        callback.reset_mock()
        
        # Modify file
        test_file.write_text("modified content")
        
        # Wait for detection
        time.sleep(0.2)
        
        # Check callback was called with modified file
        callback.assert_called_with("test-upload", {test_file})
        
    finally:
        monitor.stop_all()

def test_monitor_handles_deleted_files(tmp_upload_dir):
    """Test that monitor handles deleted files gracefully."""
    monitor = FolderMonitor(scan_interval=0.1)
    callback = Mock()
    monitor.register_callback(callback)
    
    # Create test file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Register folder to monitor
    monitor.register_folder(
        "test-upload",
        tmp_upload_dir,
        "test-bucket",
        "*.txt"
    )
    
    try:
        # Wait for initial scan
        time.sleep(0.2)
        callback.reset_mock()
        
        # Delete file
        test_file.unlink()
        
        # Wait for detection
        time.sleep(0.2)
        
        # Callback should not be called for deleted files
        callback.assert_not_called()
        
    finally:
        monitor.stop_all()

def test_monitor_stops_on_unregister(tmp_upload_dir):
    """Test that monitoring stops when folder is unregistered."""
    monitor = FolderMonitor(scan_interval=0.1)
    callback = Mock()
    monitor.register_callback(callback)
    
    # Register folder to monitor
    monitor.register_folder(
        "test-upload",
        tmp_upload_dir,
        "test-bucket",
        "*.txt"
    )
    
    # Unregister folder
    monitor.unregister_folder("test-upload")
    
    # Create new file
    test_file = tmp_upload_dir / "test.txt"
    test_file.write_text("test content")
    
    # Wait for potential detection
    time.sleep(0.2)
    
    # Callback should not be called after unregister
    callback.assert_not_called()

def test_monitor_handles_multiple_folders(tmp_path):
    """Test that monitor can handle multiple folders simultaneously."""
    monitor = FolderMonitor(scan_interval=0.1)
    callback = Mock()
    monitor.register_callback(callback)
    
    # Create test folders
    folder1 = tmp_path / "folder1"
    folder2 = tmp_path / "folder2"
    folder1.mkdir()
    folder2.mkdir()
    
    # Register both folders
    monitor.register_folder("upload1", folder1, "bucket1", "*.txt")
    monitor.register_folder("upload2", folder2, "*.txt", "bucket2")
    
    try:
        # Wait for initial setup
        time.sleep(0.2)
        callback.reset_mock()
        
        # Create files in both folders
        file1 = folder1 / "test1.txt"
        file2 = folder2 / "test2.txt"
        file1.write_text("content1")
        file2.write_text("content2")
        
        # Wait for detection
        time.sleep(0.2)
        
        # Check both files were detected
        assert callback.call_count == 2
        callback.assert_any_call("upload1", {file1})
        callback.assert_any_call("upload2", {file2})
        
    finally:
        monitor.stop_all() 