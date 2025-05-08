"""
Module containing data models for the upload service.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Any, Set

@dataclass
class UploadRequest:
    """Represents an upload request."""
    upload_id: str
    source_folder: Path
    destination_bucket: str
    pattern: str = "*"
    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        """Validate the upload request."""
        if not self.source_folder.exists():
            raise ValueError(f"Source folder {self.source_folder} does not exist")
        if not self.source_folder.is_dir():
            raise ValueError(f"{self.source_folder} is not a directory")
        if not self.upload_id:
            raise ValueError("upload_id cannot be empty")
        if not self.destination_bucket:
            raise ValueError("destination_bucket cannot be empty")

@dataclass
class UploadResult:
    """Represents the result of a single file upload."""
    file_path: Path
    s3_key: str
    success: bool
    error: Optional[str] = None
    size_bytes: Optional[int] = None
    etag: Optional[str] = None
    multipart_upload_id: Optional[str] = None
    part_number: Optional[int] = None
    offset: Optional[int] = None

@dataclass
class UploadSummary:
    """Represents a summary of an upload operation."""
    upload_id: str
    total_files: int
    successful_uploads: int
    failed_uploads: int
    results: List[UploadResult]
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class UploadProgress:
    """Represents the progress of an ongoing upload."""
    upload_id: str
    total_files: int
    completed_files: int
    failed_files: int
    in_progress_files: int
    bytes_uploaded: int
    total_bytes: int
    current_file: Optional[str] = None
    current_file_progress: Optional[float] = None
    status: str = "running"  # running, paused, completed, failed
    error: Optional[str] = None

@dataclass
class UploadState:
    """Represents the state of an upload operation."""
    upload_id: str
    source_folder: str
    destination_bucket: str
    pattern: str
    completed_files: List[str]
    in_progress_files: Dict[str, dict]  # file_path -> {upload_id, part_number, offset}
    last_modified_times: Dict[str, float]  # file_path -> mtime

@dataclass
class MonitoredFolder:
    """Represents a folder being monitored for changes."""
    source_folder: Path
    destination_path: str
    pattern: str
    last_check: float
    known_files: Set[Path] 