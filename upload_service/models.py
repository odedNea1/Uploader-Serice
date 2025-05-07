from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass
class UploadRequest:
    """Represents an upload request with all necessary metadata."""
    upload_id: str
    source_folder: Path
    destination_bucket: str
    pattern: str
    name: str
    type: str
    description: str

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

@dataclass
class UploadSummary:
    """Summary of a complete upload operation."""
    upload_id: str
    total_files: int
    successful_uploads: int
    failed_uploads: int
    results: list[UploadResult] 