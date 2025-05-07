import logging
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from .coordinator import UploadCoordinator
from .models import UploadRequest

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = typer.Typer()

@app.command()
def upload(
    source_folder: str = typer.Argument(..., help="Source folder to upload files from"),
    destination_bucket: str = typer.Argument(..., help="S3 bucket to upload to"),
    upload_id: str = typer.Option(..., help="Unique identifier for this upload"),
    pattern: str = typer.Option("**/*", help="Glob pattern for file matching"),
    name: str = typer.Option("", help="Name of the upload"),
    type: str = typer.Option("", help="Type of the upload"),
    description: str = typer.Option("", help="Description of the upload"),
    log_dir: Optional[str] = typer.Option(None, help="Directory to store log files"),
    max_workers: int = typer.Option(5, help="Maximum number of parallel uploads")
):
    """Upload files from a local folder to S3."""
    try:
        # Convert string paths to Path objects
        source_path = Path(source_folder)
        log_path = Path(log_dir) if log_dir else None
        
        # Validate source folder exists before creating request
        if not source_path.exists():
            raise ValueError(f"Source folder '{source_path}' does not exist")
            
        if not source_path.is_dir():
            raise ValueError(f"'{source_path}' is not a directory")

        # Create upload request
        request = UploadRequest(
            upload_id=upload_id,
            source_folder=source_path,
            destination_bucket=destination_bucket,
            pattern=pattern,
            name=name,
            type=type,
            description=description
        )
        
        # Process upload
        coordinator = UploadCoordinator(log_path, max_workers)
        summary = coordinator.process_upload(request)
        
        # Display results
        typer.echo("\nUpload Summary:")
        typer.echo(f"Total files: {summary.total_files}")
        typer.echo(f"Successful uploads: {summary.successful_uploads}")
        typer.echo(f"Failed uploads: {summary.failed_uploads}")
        
        if summary.failed_uploads > 0:
            typer.echo("\nFailed uploads:")
            for result in summary.results:
                if not result.success:
                    typer.echo(f"- {result.file_path}: {result.error}")
                    
        if summary.failed_uploads > 0:
            raise typer.Exit(1)
            
    except ValueError as e:
        typer.echo(f"Error: {str(e)}")
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: An unexpected error occurred: {str(e)}")
        raise typer.Exit(1)

def main():
    app() 