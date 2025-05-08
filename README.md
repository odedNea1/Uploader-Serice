Upload Service – Home Assignment

This project implements a file upload service that monitors folders and uploads matching files to S3. It supports retries, resumable uploads, and concurrent uploads using threads. The focus was on delivering a clean, minimal, working solution, while making deliberate design decisions and trade-offs under time constraints.

Design Choices and Assumptions:

MVP Persistence:
To support resumable uploads, I chose to store the state in a local JSON file. This was a quick and effective way to meet the requirement with minimal setup.
If I had more time, I would persist the state in a database, which would allow coordination across multiple machines or processes and better fault tolerance.

Retry Logic:
The tenacity library was used to automatically retry transient failures such as timeouts or service errors when uploading to S3. Retry strategies like exponential backoff were configured with sensible defaults.
With more time, I would have moved these parameters (e.g., number of retries, backoff strategy) into a config file for flexibility.

Concurrent Uploads:
File uploads are processed using a thread pool. This allows concurrent uploads within a single process. Each file upload runs in its own thread.
This is enough for the scope of the assignment, but in a production system I would consider async uploads or distributed workers for better scalability.

State File Locking:
Since multiple threads may update the state file, I added a lock to protect file writes. This works within a single process.
In a real-world setting, we'd likely need to use a transactional database or a file lock mechanism that supports cross-process safety.

File Pattern Support:
The service uses glob-style matching to detect files to upload. In the tests we used *.txt as an example, but the implementation supports any pattern. The assumption is that matching patterns are defined per upload request.

Periodic Scanning vs. Always-On Monitoring:
The system uses periodic scanning of folders (polling) instead of relying on file system watchers like inotify (Linux) or watchdog (cross-platform).
This means the process doesn’t have to keep a file handle open or maintain a persistent connection to detect changes. It can be activated periodically (e.g., via a cron job or service scheduler).
If this were a long-running daemon, a file watcher could reduce delay and CPU usage. But for simplicity, portability, and predictable behavior, scanning was chosen.

Simplicity Over Complexity:
The goal was to prioritize clarity and simplicity. For example:
    State is stored in a file, not a database.
    Metadata handling is optional.
    Chunk size and other thresholds are hardcoded for now (but could be moved to a config).

Trade-offs:
| Feature                | Chosen                      | Alternatives                     | Reason                                                 |
| ---------------------- | --------------------------- | -------------------------------- | ------------------------------------------------------ |
| State persistence      | JSON file                   | Database, Redis                  | Quick to implement, good enough for MVP                |
| Upload retries         | `tenacity`                  | Custom logic                     | Readable and reliable                                  |
| Concurrency model      | ThreadPoolExecutor          | Async, multiprocessing           | Simple to test and reason about                        |
| File scanning strategy | Periodic folder scan (glob) | Inotify, watchdog (event-driven) | Platform-independent, works without long-lived process |
| File pattern support   | Configurable glob patterns  | Regex, MIME checks               | Simple and flexible enough                             |


    Install dependencies:
poetry install

Activate virtual environment:
poetry shell

Run the CLI to register and monitor an upload:
python -m upload_service.cli register --source ./my_files --bucket my-bucket --pattern "*.txt"

Run the monitor:
python -m upload_service.cli monitor

Run tests:
    pytest -v

If I Had More Time:
    Persist state to a shared database.
    Move chunk size, scan interval, and retry configs to a YAML or .env config file.
    Use async or multi-process uploads for higher throughput.
    Build a small web UI to track progress.