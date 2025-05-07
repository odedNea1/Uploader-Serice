import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import UploadRequest, UploadResult, UploadSummary

logger = logging.getLogger(__name__)

class UploadTracker:
    """Tracks and logs upload operations."""
    
    def __init__(self, log_dir: Optional[Path] = None):
        """Initialize the upload tracker.
        
        Args:
            log_dir: Directory to store log files. If None, logs to memory only.
        """
        self.log_dir = log_dir
        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            
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