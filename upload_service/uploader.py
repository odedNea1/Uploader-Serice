"""
Module for handling S3 uploads with retry logic.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log
)

from .models import UploadResult, UploadSummary

logger = logging.getLogger(__name__)


class S3Uploader:
    """Handles file uploads to S3 with retry logic."""
    
    def __init__(self, max_workers: int = 5, chunk_size: int = 8 * 1024 * 1024):
        """Initialize the S3 uploader.
        
        Args:
            max_workers: Maximum number of concurrent upload threads
            chunk_size: Size of multipart upload chunks in bytes
        """
        self.s3_client = boto3.client('s3')
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        
    def _upload_file(self, file_path: Path, bucket: str, s3_key: str,
                     metadata: Optional[Dict[str, str]] = None) -> UploadResult:
        """Upload a single file to S3 with retries.
        
        Args:
            file_path: Path to the file to upload
            bucket: S3 bucket name
            s3_key: S3 object key
            metadata: Optional metadata to attach to the S3 object
            
        Returns:
            UploadResult object
        """
        size_bytes = file_path.stat().st_size
            
        # Use multipart upload for large files
        if size_bytes > self.chunk_size:
            return self._multipart_upload(file_path, bucket, s3_key, metadata)
        try:
            
            
            # Simple upload for small files
            extra_args = {'Metadata': metadata} if metadata else {}
            self._upload_file_with_retries(file_path, bucket, s3_key, extra_args)
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=True,
                error=None,
                size_bytes=size_bytes
            )
            
        except Exception as e:
            logger.error(f"Error uploading {file_path} to {s3_key}: {e}")
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=False,
                error=str(e),
                size_bytes=size_bytes,
                etag=None
            )
            
    def _multipart_upload(self, file_path: Path, bucket: str, s3_key: str,
                         metadata: Optional[Dict[str, str]] = None) -> UploadResult:
        """Handle multipart upload for large files with retries.
        
        Args:
            file_path: Path to the file to upload
            bucket: S3 bucket name
            s3_key: S3 object key
            metadata: Optional metadata to attach to the S3 object
            
        Returns:
            UploadResult object
        """
        size_bytes = file_path.stat().st_size
        extra_args = {'Metadata': metadata} if metadata else {}
        try:
            return self._multipart_upload_with_retries(file_path, bucket, s3_key, extra_args)
        except Exception as e:
            logger.error(f"Error in multipart upload for {file_path} to {s3_key}: {e}")
            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=False,
                error=str(e),
                size_bytes=size_bytes
            )
            
    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before=before_log(logger, logging.DEBUG),
        after=after_log(logger, logging.DEBUG)
    )
    def _upload_part(self, bucket: str, key: str, upload_id: str,
                    part_number: int, data: bytes) -> Dict[str, Any]:
        """Upload a single part of a multipart upload with retries.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            upload_id: Multipart upload ID
            part_number: Part number
            data: Part data bytes
            
        Returns:
            Response from S3 upload_part call
        """
        return self.s3_client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=data
        )
        
    def upload_files(self, files: List[Path], bucket: str,
                    destination_prefix: str = "",
                    metadata: Optional[Dict[str, str]] = None) -> UploadSummary:
        """Upload multiple files concurrently.
        
        Args:
            files: List of file paths to upload
            bucket: S3 bucket name
            destination_prefix: Optional prefix for S3 keys
            metadata: Optional metadata to attach to all objects
            
        Returns:
            UploadSummary object
        """
        results = []
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(
                    self._upload_file,
                    file_path,
                    bucket,
                    str(Path(destination_prefix) / file_path.name),
                    metadata
                ): file_path
                for file_path in files
            }
            
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.success:
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    file_path = future_to_file[future]
                    logger.error(f"Unexpected error uploading {file_path}: {e}")
                    results.append(UploadResult(
                        file_path=file_path,
                        s3_key=str(Path(destination_prefix) / file_path.name),
                        success=False,
                        error=str(e),
                        size_bytes=file_path.stat().st_size,
                        etag=None
                    ))
                    failed += 1
                    
        return UploadSummary(
            upload_id=str(hash(tuple(files))),
            total_files=len(files),
            successful_uploads=successful,
            failed_uploads=failed,
            results=results
        ) 
    
    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        before=before_log(logger, logging.DEBUG),
        after=after_log(logger, logging.DEBUG)
    )
    def _upload_file_with_retries(self, file_path: Path, bucket: str, s3_key: str, extra_args: Dict[str, str]) -> None:
        self.s3_client.upload_file(str(file_path), bucket, s3_key, ExtraArgs=extra_args)

    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        before=before_log(logger, logging.DEBUG),
        after=after_log(logger, logging.DEBUG)
    )
    def _multipart_upload_with_retries(self, file_path: Path, bucket: str, s3_key: str, extra_args: Dict[str, str]) -> UploadResult:
        size_bytes = file_path.stat().st_size
        mpu = self.s3_client.create_multipart_upload(Bucket=bucket, Key=s3_key, **extra_args)
        upload_id = mpu['UploadId']
        parts = []
        offset = 0
        part_number = 1

        try:
            with open(file_path, 'rb') as f:
                while offset < size_bytes:
                    chunk = f.read(self.chunk_size)
                    part = self._upload_part(bucket, s3_key, upload_id, part_number, chunk)
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                    offset += len(chunk)
                    part_number += 1

            self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            return UploadResult(
                file_path=file_path,
                s3_key=s3_key,
                success=True,
                error=None,
                size_bytes=size_bytes,
                etag=parts[-1]['ETag'] if parts else None,
                multipart_upload_id=upload_id
            )

        except Exception:
            self.s3_client.abort_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id)
            raise