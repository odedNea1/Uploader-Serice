"""
Module for handling periodic folder monitoring and change detection.
"""
import threading
import time
import logging
from pathlib import Path
from typing import Dict, Set, Optional, Callable
from .models import MonitoredFolder

logger = logging.getLogger(__name__)


class FolderMonitor:
    """Handles periodic monitoring of registered folders for changes."""
    
    def __init__(self, scan_interval: int = 30):
        self._monitors: Dict[str, MonitoredFolder] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._monitor_threads: Dict[str, threading.Thread] = {}
        self._scan_interval = scan_interval
        self._callback: Optional[Callable] = None
        self._lock = threading.Lock()

    def register_callback(self, callback: Callable[[str, Set[Path]], None]) -> None:
        """Register a callback to be called when changes are detected.
        
        Args:
            callback: Function to call with (upload_id, changed_files) when changes detected
        """
        self._callback = callback

    def register_folder(self, folder: MonitoredFolder) -> None:
        """Register a new folder to monitor.
        
        Args:
            folder: MonitoredFolder configuration
        """
        with self._lock:
            if folder.upload_id in self._monitors:
                logger.warning(f"Upload ID {folder.upload_id} already being monitored")
                return

            self._monitors[folder.upload_id] = folder
            
            stop_event = threading.Event()
            self._stop_events[folder.upload_id] = stop_event
            
            thread = threading.Thread(
                target=self._monitor_folder,
                args=(folder.upload_id, stop_event),
                name=f"monitor-{folder.upload_id}",
                daemon=True
            )
            self._monitor_threads[folder.upload_id] = thread
            thread.start()
            
            logger.info(f"Started monitoring folder {folder.source_folder} for upload {folder.upload_id}")

    def unregister_folder(self, upload_id: str) -> None:
        """Stop monitoring a folder.
        
        Args:
            upload_id: The upload ID to stop monitoring
        """
        with self._lock:
            if upload_id not in self._monitors:
                logger.warning(f"Upload ID {upload_id} not being monitored")
                return

            if upload_id in self._stop_events:
                self._stop_events[upload_id].set()
                self._monitor_threads[upload_id].join(timeout=5)
                
            self._monitors.pop(upload_id, None)
            self._stop_events.pop(upload_id, None)
            self._monitor_threads.pop(upload_id, None)
            
            logger.info(f"Stopped monitoring upload {upload_id}")

    def _monitor_folder(self, upload_id: str, stop_event: threading.Event) -> None:
        """Background thread that periodically checks for folder changes.
        
        Args:
            upload_id: The upload ID being monitored
            stop_event: Event to signal thread termination
        """
        while not stop_event.is_set():
            try:
                with self._lock:
                    if upload_id not in self._monitors:
                        break
                        
                    monitor = self._monitors[upload_id]
                    current_files = set(p for p in monitor.source_folder.glob(monitor.pattern) 
                                     if p.is_file())
                    
                    # Check for new or modified files
                    changed_files = set()
                    for file in current_files:
                        if (file not in monitor.known_files or 
                            file.stat().st_mtime > monitor.last_check):
                            changed_files.add(file)
                    
                    if changed_files and self._callback:
                        self._callback(upload_id, changed_files)
                        
                    # Update state
                    monitor.known_files = current_files
                    monitor.last_check = time.time()
                    
            except Exception as e:
                logger.error(f"Error monitoring folder for upload {upload_id}: {e}")
            finally:
                logger.info(f"thread exiting: {upload_id}")
                
            stop_event.wait(self._scan_interval)

    def stop_all(self) -> None:
        # Copy keys first while holding the lock
        with self._lock:
            upload_ids = list(self._monitors.keys())

        # Call unregister_folder outside the lock to avoid deadlock
        for upload_id in upload_ids:
            self.unregister_folder(upload_id)
