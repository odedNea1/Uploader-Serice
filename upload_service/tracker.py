"""
Module for tracking and persisting upload operations state.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Set
from dataclasses import asdict
import threading
from .models import UploadRequest, UploadResult, UploadSummary, UploadState

logger = logging.getLogger(__name__)


class UploadTracker:
    """Tracks and persists upload operations state."""
    
    def __init__(self, log_dir: Optional[Path] = None, state_file: Optional[Path] = None):
        """Initialize the upload tracker.
        
        Args:
            log_dir: Directory to store log files. If None, logs to memory only.
            state_file: Path to the state persistence JSON file.
        """
        self.log_dir = log_dir
        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            
        self.state_file = state_file or (log_dir / "upload_state.json" if log_dir else None)
        self._upload_states: Dict[str, UploadState] = {}
        self._lock = threading.Lock()
        
        # Load existing state if available
        self._load_state()

    def _load_state(self) -> None:
        """Load upload states from the state file."""
        if not self.state_file or not self.state_file.exists():
            return
            
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                
            for state_dict in data.get('upload_states', []):
                state = UploadState(
                    upload_id=state_dict['upload_id'],
                    source_folder=state_dict['source_folder'],
                    destination_bucket=state_dict['destination_bucket'],
                    pattern=state_dict['pattern'],
                    completed_files=state_dict.get('completed_files', []),
                    in_progress_files=state_dict.get('in_progress_files', {}),
                    last_modified_times=state_dict.get('last_modified_times', {})
                )
                self._upload_states[state.upload_id] = state
                
            logger.info(f"Loaded {len(self._upload_states)} upload states from {self.state_file}")
        except Exception as e:
            logger.error(f"Error loading state file: {e}")

    def _save_state(self) -> None:
        """Save current upload states to the state file."""
        if not self.state_file:
            return
            
        try:
            data = {
                'upload_states': [
                    asdict(state) for state in self._upload_states.values()
                ]
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.debug(f"Saved {len(self._upload_states)} upload states to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def get_upload_state(self, upload_id: str) -> Optional[UploadState]:
        """Get the current state of an upload.
        
        Args:
            upload_id: Unique identifier for the upload
            
        Returns:
            UploadState if found, None otherwise
        """
        with self._lock:
            return self._upload_states.get(upload_id)

    def register_upload(self, request: UploadRequest) -> None:
        """Register a new upload and initialize its state.
        
        Args:
            request: UploadRequest object
        """
        with self._lock:
            state = UploadState(
                upload_id=request.upload_id,
                source_folder=str(request.source_folder),
                destination_bucket=request.destination_bucket,
                pattern=request.pattern,
                completed_files=[],
                in_progress_files={},
                last_modified_times={}
            )
            self._upload_states[request.upload_id] = state
            self._save_state()
        
        self.log_upload_request(request)

    def mark_file_complete(self, upload_id: str, file_path: str, 
                          result: UploadResult) -> None:
        """Mark a file as successfully uploaded.
        
        Args:
            upload_id: Unique identifier for the upload
            file_path: Path to the completed file
            result: UploadResult for the file
        """
        with self._lock:
            if state := self._upload_states.get(upload_id):
                if file_path in state.in_progress_files:
                    del state.in_progress_files[file_path]
                    
                if result.success and file_path not in state.completed_files:
                    state.completed_files.append(file_path)
                    state.last_modified_times[file_path] = Path(file_path).stat().st_mtime
                    
                self._save_state()

    def register_multipart_upload(self, upload_id: str, file_path: str,
                                s3_upload_id: str, part_number: int,
                                offset: int) -> None:
        """Register an in-progress multipart upload.
        
        Args:
            upload_id: Unique identifier for the upload
            file_path: Path to the file being uploaded
            s3_upload_id: S3 multipart upload ID
            part_number: Current part number
            offset: Current byte offset in the file
        """
        with self._lock:
            if state := self._upload_states.get(upload_id):
                state.in_progress_files[file_path] = {
                    'upload_id': s3_upload_id,
                    'part_number': part_number,
                    'offset': offset
                }
                self._save_state()

    def get_incomplete_files(self, upload_id: str) -> Set[str]:
        """Get files that need to be uploaded or resumed.
        
        Args:
            upload_id: Unique identifier for the upload
            
        Returns:
            Set of file paths that need attention
        """
        with self._lock:
            if state := self._upload_states.get(upload_id):
                source_folder = Path(state.source_folder)
                current_files = set(str(p) for p in source_folder.glob(state.pattern)
                                  if p.is_file())
                
                # Files that need attention:
                # 1. Files that exist but haven't been completed
                # 2. Completed files that have been modified
                # 3. In-progress files
                incomplete = set()
                
                for file_path in current_files:
                    path = Path(file_path)
                    current_mtime = path.stat().st_mtime
                    
                    if (file_path not in state.completed_files or
                        current_mtime != state.last_modified_times.get(file_path, 0)):
                        incomplete.add(file_path)
                        
                incomplete.update(state.in_progress_files.keys())
                return incomplete
                
            return set()

    def _get_log_path(self, upload_id: str) -> Optional[Path]:
        """Get the path for the log file of a specific upload.
        
        Args:
            upload_id: Unique identifier for the upload
            
        Returns:
            Path to the log file, or None if logging to memory
        """
        if not self.log_dir:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.log_dir / f"upload_{upload_id}_{timestamp}.json"
        
    def log_upload_request(self, request: UploadRequest) -> None:
        """Log the upload request details.
        
        Args:
            request: UploadRequest object
        """
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "upload_id": request.upload_id,
            "source_folder": str(request.source_folder),
            "destination_bucket": request.destination_bucket,
            "pattern": request.pattern,
            "metadata": {
                "name": request.name,
                "type": request.type,
                "description": request.description
            }
        }
        
        if log_path := self._get_log_path(request.upload_id):
            with open(log_path, 'w') as f:
                json.dump(log_data, f, indent=2)
                
        logger.info(f"Starting upload {request.upload_id}")
        
    def log_upload_summary(self, summary: UploadSummary) -> None:
        """Log the summary of a completed upload operation.
        
        Args:
            summary: UploadSummary object
        """
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "upload_id": summary.upload_id,
            "total_files": summary.total_files,
            "successful_uploads": summary.successful_uploads,
            "failed_uploads": summary.failed_uploads,
            "results": [
                {
                    "file_path": str(r.file_path),
                    "s3_key": r.s3_key,
                    "success": r.success,
                    "error": r.error,
                    "size_bytes": r.size_bytes,
                    "etag": r.etag
                }
                for r in summary.results
            ]
        }
        
        if log_path := self._get_log_path(summary.upload_id):
            with open(log_path, 'a') as f:
                json.dump(log_data, f, indent=2)
                
        logger.info(
            f"Completed upload {summary.upload_id}: "
            f"{summary.successful_uploads}/{summary.total_files} files uploaded successfully"
        ) 