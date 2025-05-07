import logging
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

class FileScanner:
    """Handles file discovery using glob patterns."""
    
    def __init__(self, source_folder: Path, pattern: str):
        """Initialize the scanner with source folder and glob pattern.
        
        Args:
            source_folder: Root directory to scan
            pattern: Glob pattern to match files
        """
        self.source_folder = source_folder
        self.pattern = pattern
        
    def scan_files(self) -> Iterator[Path]:
        """Scan the source folder recursively for files matching the pattern.
        
        Yields:
            Path objects for each matching file
        """
        logger.info(f"Scanning {self.source_folder} for files matching pattern: {self.pattern}")
        
        try:
            for file_path in self.source_folder.rglob(self.pattern):
                if file_path.is_file():
                    logger.debug(f"Found matching file: {file_path}")
                    yield file_path
        except Exception as e:
            logger.error(f"Error scanning files: {str(e)}")
            raise
            
    def get_relative_path(self, file_path: Path) -> Path:
        """Get the relative path of a file from the source folder.
        
        Args:
            file_path: Absolute path of the file
            
        Returns:
            Path object representing the relative path
        """
        try:
            return file_path.relative_to(self.source_folder)
        except ValueError:
            logger.error(f"File {file_path} is not under source folder {self.source_folder}")
            raise 