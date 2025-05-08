"""
Module for coordinating upload operations with monitoring and resumability.
"""
import logging
from pathlib import Path
from typing import Optional, Dict, Set
import threading

from .models import UploadRequest
from .scanner import FileScanner
from .uploader import S3Uploader
from .tracker import UploadTracker
from .monitor import FolderMonitor

logger = logging.getLogger(__name__)

class UploadCoordinator:
    """Coordinates file uploads with monitoring and resumability."""
    
    def __init__(self, log_dir: Optional[Path] = None,
                 state_file: Optional[Path] = None,
                 scan_interval: int = 30):
        """Initialize the upload coordinator.
        
        Args:
            log_dir: Directory for log files
            state_file: Path to state persistence file
            scan_interval: Interval in seconds for folder monitoring
        """
        self.scanner = FileScanner()
        self.tracker = UploadTracker(log_dir=log_dir, state_file=state_file)
        self.monitor = FolderMonitor(scan_interval=scan_interval)
        self._active_uploads: Dict[str, S3Uploader] = {}
        self._lock = threading.Lock()
        
        # Register callback for file changes
        self.monitor.register_callback(self._handle_file_changes)
        
        # Resume any incomplete uploads
        self._resume_incomplete_uploads()
        
    def _resume_incomplete_uploads(self) -> None:
        """Resume any incomplete uploads from the persisted state."""
        for upload_id in self.tracker._upload_states:
            state = self.tracker.get_upload_state(upload_id)
            if not state:
                continue
                
            logger.info(f"Resuming upload {upload_id} from {state.source_folder}")
            
            # Create uploader for this upload
            uploader = S3Uploader()
            self._active_uploads[upload_id] = uploader
            
            # Register folder for monitoring
            self.monitor.register_folder(
                upload_id,
                Path(state.source_folder),
                state.destination_bucket,
                state.pattern
            )
            
            # Process any incomplete files
            incomplete_files = self.tracker.get_incomplete_files(upload_id)
            if incomplete_files:
                self._process_files(
                    upload_id,
                    [Path(f) for f in incomplete_files],
                    state.destination_bucket
                )
                
    def _handle_file_changes(self, upload_id: str, changed_files: Set[Path]) -> None:
        """Handle detected file changes in monitored folders.
        
        Args:
            upload_id: ID of the upload task
            changed_files: Set of files that changed
        """
        if state := self.tracker.get_upload_state(upload_id):
            logger.info(f"Processing {len(changed_files)} changed files for upload {upload_id}")
            self._process_files(upload_id, list(changed_files), state.destination_bucket)
            
    def _process_files(self, upload_id: str, files: list[Path],
                      destination_bucket: str) -> None:
        """Process a list of files for upload.
        
        Args:
            upload_id: ID of the upload task
            files: List of files to process
            destination_bucket: S3 bucket name
        """
        if not files:
            return
            
        with self._lock:
            if upload_id not in self._active_uploads:
                logger.warning(f"No active uploader for {upload_id}")
                return
                
            uploader = self._active_uploads[upload_id]
            
            # Upload files
            summary = uploader.upload_files(
                files,
                destination_bucket
            )
            
            # Update tracker with results
            for result in summary.results:
                if result.success:
                    self.tracker.mark_file_complete(
                        upload_id,
                        str(result.file_path),
                        result
                    )
                    
            # Log summary
            self.tracker.log_upload_summary(summary)
            
    def start_upload(self, request: UploadRequest) -> None:
        """Start a new upload task with monitoring.
        
        Args:
            request: Upload request details
        """
        # Validate request
        if not request.source_folder.exists():
            raise ValueError(f"Source folder does not exist: {request.source_folder}")
            
        # Register upload with tracker
        self.tracker.register_upload(request)
        
        # Create uploader
        uploader = S3Uploader()
        with self._lock:
            self._active_uploads[request.upload_id] = uploader
            
        # Start folder monitoring
        self.monitor.register_folder(
            request.upload_id,
            request.source_folder,
            request.destination_bucket,
            request.pattern
        )
        
        # Initial scan and upload
        files = self.scanner.scan_folder(request.source_folder, request.pattern)
        if files:
            self._process_files(
                request.upload_id,
                files,
                request.destination_bucket
            )
            
    def stop_upload(self, upload_id: str) -> None:
        """Stop an upload task and its monitoring.
        
        Args:
            upload_id: ID of the upload to stop
        """
        # Stop monitoring
        self.monitor.unregister_folder(upload_id)
        
        # Remove from active uploads
        with self._lock:
            self._active_uploads.pop(upload_id, None)
            
        logger.info(f"Stopped upload task {upload_id}")
        
    def stop_all(self) -> None:
        """Stop all upload tasks and monitoring."""
        self.monitor.stop_all()
        with self._lock:
            self._active_uploads.clear() 