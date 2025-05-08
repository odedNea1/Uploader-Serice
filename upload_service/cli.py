"""
Command-line interface for the upload service.
"""
import argparse
import logging
import sys
import uuid
from pathlib import Path
import signal
import json
from typing import Optional

from .coordinator import UploadCoordinator
from .models import UploadRequest

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the application.
    
    Args:
        verbose: Whether to enable debug logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def load_config(config_file: Optional[Path] = None) -> dict:
    """Load configuration from a JSON file.
    
    Args:
        config_file: Path to config file
        
    Returns:
        Dictionary of configuration values
    """
    if not config_file:
        return {}
        
    try:
        with open(config_file) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return {}

def create_coordinator(args: argparse.Namespace) -> UploadCoordinator:
    """Create and configure the upload coordinator.
    
    Args:
        args: Command line arguments
        
    Returns:
        Configured UploadCoordinator instance
    """
    config = load_config(args.config)
    
    log_dir = Path(config.get('log_dir', 'logs'))
    state_file = Path(config.get('state_file', 'upload_state.json'))
    scan_interval = config.get('scan_interval', 30)
    
    return UploadCoordinator(
        log_dir=log_dir,
        state_file=state_file,
        scan_interval=scan_interval
    )

def handle_start(args: argparse.Namespace) -> None:
    """Handle the start command.
    
    Args:
        args: Command line arguments
    """
    coordinator = create_coordinator(args)
    
    request = UploadRequest(
        upload_id=args.upload_id or str(uuid.uuid4()),
        source_folder=Path(args.source_folder),
        destination_bucket=args.bucket,
        pattern=args.pattern,
        name=args.name,
        type=args.type,
        description=args.description
    )
    
    try:
        coordinator.start_upload(request)
        logger.info(f"Started upload {request.upload_id}")
        
        # Keep running until interrupted
        signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
        signal.pause()
        
    except KeyboardInterrupt:
        logger.info("Upload interrupted by user")
        coordinator.stop_upload(request.upload_id)
        coordinator.stop_all()
        
    except Exception as e:
        logger.error(f"Error starting upload: {e}")
        sys.exit(1)

def handle_stop(args: argparse.Namespace) -> None:
    """Handle the stop command.
    
    Args:
        args: Command line arguments
    """
    coordinator = create_coordinator(args)
    coordinator.stop_upload(args.upload_id)
    logger.info(f"Stopped upload {args.upload_id}")

def handle_list(args: argparse.Namespace) -> None:
    """Handle the list command.
    
    Args:
        args: Command line arguments
    """
    coordinator = create_coordinator(args)
    
    # Get active uploads from state
    if hasattr(coordinator.tracker, '_upload_states'):
        for upload_id, state in coordinator.tracker._upload_states.items():
            print(f"\nUpload ID: {upload_id}")
            print(f"Source: {state.source_folder}")
            print(f"Destination: {state.destination_bucket}")
            print(f"Pattern: {state.pattern}")
            print(f"Completed Files: {len(state.completed_files)}")
            print(f"In Progress: {len(state.in_progress_files)}")
    else:
        print("No active uploads found")

def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="S3 Upload Service CLI")
    parser.add_argument('-v', '--verbose', action='store_true',
                       help="Enable verbose logging")
    parser.add_argument('-c', '--config', type=Path,
                       help="Path to config file")
                       
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Start command
    start_parser = subparsers.add_parser('start',
                                        help="Start a new upload task")
    start_parser.add_argument('source_folder', type=str,
                            help="Source folder path")
    start_parser.add_argument('bucket', type=str,
                            help="Destination S3 bucket")
    start_parser.add_argument('-i', '--upload-id', type=str,
                            help="Custom upload ID")
    start_parser.add_argument('-p', '--pattern', type=str,
                            default="*", help="File pattern to match")
    start_parser.add_argument('-n', '--name', type=str,
                            help="Upload name")
    start_parser.add_argument('-t', '--type', type=str,
                            help="Upload type")
    start_parser.add_argument('-d', '--description', type=str,
                            help="Upload description")
                            
    # Stop command
    stop_parser = subparsers.add_parser('stop',
                                       help="Stop an upload task")
    stop_parser.add_argument('upload_id', type=str,
                           help="Upload ID to stop")
                           
    # List command
    subparsers.add_parser('list',
                         help="List active uploads")
                         
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    try:
        if args.command == 'start':
            handle_start(args)
        elif args.command == 'stop':
            handle_stop(args)
        elif args.command == 'list':
            handle_list(args)
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 