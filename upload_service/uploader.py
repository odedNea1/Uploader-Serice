import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .models import UploadResult

logger = logging.getLogger(__name__)

class S3Uploader:
    """Handles file uploads to S3 with validation."""
    
    def __init__(self, bucket_name: str, upload_id: str, max_workers: int = 5):
        """Initialize the S3 uploader.
        
        Args:
            bucket_name: Name of the S3 bucket
            upload_id: Unique identifier for this upload batch
            max_workers: Maximum number of parallel uploads
        """
        self.bucket_name = bucket_name
        self.upload_id = upload_id
        self.max_workers = max_workers
        self.s3_client = boto3.client('s3')
        
    def _upload_file(self, file_path: Path, s3_key: str) -> UploadResult:
        """Upload a single file to S3 and validate the upload.
        
        Args:
            file_path: Local path to the file
            s3_key: S3 key to upload to
            
        Returns:
            UploadResult with upload status and metadata
        """
        try:
            # Upload file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key
            )
            
            # Get file size
            size_bytes = file_path.stat().st_size
            
            # Get ETag for validation
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            etag = response['ETag'].strip('"')
            
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=True,
                size_bytes=size_bytes,
                etag=etag
            )
            
        except ClientError as e:
            error_msg = f"S3 upload error: {str(e)}"
            logger.error(error_msg)
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=False,
                error=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=False,
                error=error_msg
            )
            
    def upload_files(self, files: list[tuple[Path, str]]) -> list[UploadResult]:
        """Upload multiple files to S3 in parallel.
        
        Args:
            files: List of (file_path, s3_key) tuples
            
        Returns:
            List of UploadResult objects
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._upload_file, file_path, s3_key)
                for file_path, s3_key in files
            ]
            
            for future in futures:
                result = future.result()
                results.append(result)
                
                if result.success:
                    logger.info(f"Successfully uploaded {result.file_path} to {result.s3_key}")
                else:
                    logger.error(f"Failed to upload {result.file_path}: {result.error}")
                    
        return results 