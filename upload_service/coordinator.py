import logging
from pathlib import Path
from typing import Optional

from .models import UploadRequest, UploadResult, UploadSummary
from .scanner import FileScanner
from .tracker import UploadTracker
from .uploader import S3Uploader

logger = logging.getLogger(__name__)

class UploadCoordinator:
    """Coordinates the upload process by managing all components."""
    
    def __init__(
        self,
        log_dir: Optional[Path] = None,
        max_workers: int = 5
    ):
        """Initialize the upload coordinator.
        
        Args:
            log_dir: Directory to store log files
            max_workers: Maximum number of parallel uploads
        """
        self.tracker = UploadTracker(log_dir)
        self.max_workers = max_workers
        
    def process_upload(self, request: UploadRequest) -> UploadSummary:
        """Process an upload request.
        
        Args:
            request: UploadRequest object
            
        Returns:
            UploadSummary with results of the upload operation
        """
        # Log the start of the upload
        self.tracker.log_upload_request(request)
        
        # Initialize components
        scanner = FileScanner(request.source_folder, request.pattern)
        uploader = S3Uploader(
            request.destination_bucket,
            request.upload_id,
            max_workers=self.max_workers
        )
        
        # Scan for files
        files_to_upload = []
        for file_path in scanner.scan_files():
            relative_path = scanner.get_relative_path(file_path)
            s3_key = f"{request.upload_id}/{relative_path}"
            files_to_upload.append((file_path, s3_key))
            
        if not files_to_upload:
            logger.warning(f"No files found matching pattern {request.pattern} in {request.source_folder}")
            
        # Upload files
        results = uploader.upload_files(files_to_upload)
        
        # Create summary
        successful_uploads = sum(1 for r in results if r.success)
        summary = UploadSummary(
            upload_id=request.upload_id,
            total_files=len(results),
            successful_uploads=successful_uploads,
            failed_uploads=len(results) - successful_uploads,
            results=results
        )
        
        # Log summary
        self.tracker.log_upload_summary(summary)
        
        return summary 