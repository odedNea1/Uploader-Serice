"""
Module for scanning folders and detecting file changes.
"""
import logging
from pathlib import Path
from typing import List, Set, Dict, Optional

from .models import UploadRequest

logger = logging.getLogger(__name__)

class FileScanner:
    """Scans folders for files and tracks changes."""
    
    def __init__(self):
        """Initialize the file scanner."""
        self._cache: Dict[str, Dict[Path, float]] = {}
        
    def scan(self, request: UploadRequest) -> List[Path]:
        """Scan a folder for files matching the request pattern.
        
        Args:
            request: Upload request containing folder and pattern
            
        Returns:
            List of file paths found
        """
        return self.scan_folder(request.source_folder, request.pattern)
        
    def scan_folder(self, folder: Path, pattern: str = "*") -> List[Path]:
        """Scan a folder for files matching the pattern.
        
        Args:
            folder: Path to the folder to scan
            pattern: Glob pattern to match files against
            
        Returns:
            List of file paths found
        """
        if not folder.exists():
            logger.error(f"Folder does not exist: {folder}")
            return []
            
        try:
            return [p for p in folder.glob(pattern) if p.is_file()]
        except Exception as e:
            logger.error(f"Error scanning folder {folder}: {e}")
            return []
            
    def get_changes(self, folder: Path, pattern: str = "*") -> Set[Path]:
        """Get files that have changed since last scan.
        
        Args:
            folder: Path to the folder to scan
            pattern: Glob pattern to match files against
            
        Returns:
            Set of changed file paths
        """
        cache_key = f"{folder}:{pattern}"
        previous = self._cache.get(cache_key, {})
        current = {}
        changed = set()
        
        try:
            # Scan current state
            for path in folder.glob(pattern):
                if path.is_file():
                    mtime = path.stat().st_mtime
                    current[path] = mtime
                    
                    # Check if file is new or modified
                    if path not in previous or mtime > previous[path]:
                        changed.add(path)
                        
            # Update cache
            self._cache[cache_key] = current
            
            return changed
            
        except Exception as e:
            logger.error(f"Error checking for changes in {folder}: {e}")
            return set()
            
    def clear_cache(self, folder: Optional[Path] = None,
                   pattern: Optional[str] = None) -> None:
        """Clear the file modification cache.
        
        Args:
            folder: Optional folder to clear cache for
            pattern: Optional pattern to clear cache for
        """
        if folder and pattern:
            self._cache.pop(f"{folder}:{pattern}", None)
        else:
            self._cache.clear()
            
    def get_relative_path(self, file_path: Path, base_path: Path) -> Path:
        """Get the relative path of a file from a base path.
        
        Args:
            file_path: Path to the file
            base_path: Base path to make relative to
            
        Returns:
            Relative path from base_path to file_path
        """
        try:
            return file_path.relative_to(base_path)
        except ValueError:
            logger.error(f"File {file_path} is not relative to {base_path}")
            return file_path 