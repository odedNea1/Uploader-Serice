# Upload Service

A production-grade Python service for uploading files to Amazon S3 with parallel upload support, logging, and validation.

## Features

- Recursive file scanning with glob pattern support
- Parallel file uploads using ThreadPoolExecutor
- Comprehensive logging and tracking
- S3 upload validation (ETag, file size)
- Clean CLI interface using Typer
- Type hints and modern Python practices
- Comprehensive test coverage

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd upload-service
```

2. Install dependencies:
```bash
pip install -e .
```

## Configuration

The service uses environment variables for AWS credentials. Create a `.env` file in the project root:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

## Usage

### CLI

Upload files using the command-line interface:

```bash
python -m upload_service upload \
    /path/to/source/folder \
    destination-bucket \
    --upload-id unique-id \
    --pattern "**/*.txt" \
    --name "My Upload" \
    --type "data" \
    --description "Test upload" \
    --log-dir /path/to/logs \
    --max-workers 5
```

### Python API

Use the service programmatically:

```python
from pathlib import Path
from upload_service import UploadCoordinator, UploadRequest

coordinator = UploadCoordinator(log_dir=Path("/path/to/logs"))
request = UploadRequest(
    upload_id="unique-id",
    source_folder=Path("/path/to/source/folder"),
    destination_bucket="destination-bucket",
    pattern="**/*.txt",
    name="My Upload",
    type="data",
    description="Test upload"
)

summary = coordinator.process_upload(request)
print(f"Uploaded {summary.successful_uploads}/{summary.total_files} files")
```

## Testing

Run the test suite:

```bash
pytest tests/
```

## Project Structure

```
upload_service/
├── __init__.py
├── coordinator.py    # Main orchestrator
├── scanner.py       # File discovery
├── uploader.py      # S3 upload handling
├── tracker.py       # Logging and tracking
├── models.py        # Data structures
└── cli.py           # Command-line interface
tests/
└── test_upload_service.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 