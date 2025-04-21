# ZFS Sync Py - ZFS Dataset Replication Tool (Python Version)

A Python-based utility with an optional Textual TUI for reliable ZFS dataset replication between hosts, leveraging the power of `sanoid` and `syncoid`.

## Features

-   **Sanoid/Syncoid Integration:** Uses the robust `sanoid` for snapshot creation/pruning based on policy and `syncoid` for efficient and reliable data replication.
-   **Flexible Configuration:** Configure jobs and global settings via a `.env` file.
-   **Local & Remote Transfers:** Supports transfers between local datasets and remote hosts via SSH.
-   **Textual TUI (Optional):** Provides an interactive interface for selecting jobs and monitoring progress (requires `textual`).
-   **Command-Line Operation:** Can be run non-interactively via command-line arguments, suitable for scripting.
-   **Prerequisite Checks:** Verifies necessary tools (`zfs`, `ssh`, `perl`).
-   **Dry Run Mode:** Allows previewing actions without making changes.
-   **Configurable Options:** Supports compression (via `syncoid`), bandwidth limiting, resume, recursive transfers, custom SSH options, etc.

## Project Structure

```
zfs_sync/
├── libs/                     # Git submodule directory
│   └── sanoid/               # Sanoid/Syncoid submodule
├── app/                      # Main application code
│   ├── tui/                  # Textual TUI components
│   │   ├── screens/          # TUI screen definitions
│   │   ├── __init__.py
│   │   ├── app.py            # Main Textual App class
│   │   └── messages.py       # TUI messages
│   ├── __init__.py
│   ├── config.py             # Configuration loading (.env based)
│   ├── transfer.py           # Syncoid execution logic
│   ├── utils.py              # Utility functions (logging, cmd execution)
│   └── zfs.py                # Basic ZFS helper functions (dataset checks)
├── zfs_sync.py               # Main script entry point
├── start.sh                  # Script to setup venv and run
├── requirements.txt          # Python dependencies
├── .env.sample               # Sample configuration file
├── .gitignore
├── .gitmodules               # Submodule configuration
└── README.md                 # This file
```

## Prerequisites

-   **Python 3:** Required to run the script.
-   **ZFS:** Installed on both source and destination systems where datasets reside.
-   **Perl:** Required by `sanoid` and `syncoid`.
-   **Sanoid/Syncoid:** Included as a submodule in `libs/sanoid`. Ensure the scripts (`libs/sanoid/sanoid`, `libs/sanoid/syncoid`) are executable (`chmod +x`). Dependencies like `pv` or `mbuffer` might be needed by `syncoid` depending on the options used (e.g., `--progress`, `--mbuffer-size`).
-   **SSH:** Required for remote transfers. Key-based authentication is strongly recommended.
-   **(Optional) Textual:** Required for the interactive TUI mode (`pip install textual`).

## Configuration (`.env` file)

Copy `.env.sample` to `.env` and modify the settings:

```bash
cp .env.sample .env
nano .env
```

**Global Settings:**

-   `ZFS_SYNC_JOB_NAMES`: Space-separated list of job names to define (e.g., `ZFS_SYNC_JOB_NAMES="job1 job2"`)
-   `DEFAULT_SSH_USER`: Default SSH user for jobs (default: `root`)
-   `DEFAULT_SNAPSHOT_PREFIX`: Default prefix for snapshots created by *this tool* (if `create_source_snapshot` is enabled, default: `backup`). Note: `sanoid` uses its own prefix defined in `sanoid.conf`.
-   `DEFAULT_RECURSIVE`: Default recursive setting for jobs (`true`/`false`, default: `true`)
-   `DEFAULT_USE_COMPRESSION`: Default compression setting for `syncoid` (`true`/`false`, default: `true`)
-   `DEFAULT_RESUME_SUPPORT`: Default resume setting for `syncoid` (`true`/`false`, default: `true`). `syncoid` resumes by default; set to `false` to add `--no-resume`.
-   `LOG_LEVEL`: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, default: `INFO`)
-   `LOG_FILE`: Path to log file (optional, logs to console otherwise)
-   `DRY_RUN`: Global dry run flag (`true`/`false`, default: `false`). Overridden by `--dry-run` argument.
-   `SSH_TIMEOUT`: SSH connection timeout in seconds (default: `10`)
-   `SSH_EXTRA_OPTIONS`: Additional options to pass to SSH (e.g., `-i /path/to/key -p 2222`)
-   `SANOID_PATH`: Path to `sanoid` executable (default: `libs/sanoid/sanoid`)
-   `SYNCOID_PATH`: Path to `syncoid` executable (default: `libs/sanoid/syncoid`)
-   `SANOID_CONF_PATH`: Path to `sanoid.conf` file (default: `/etc/sanoid/sanoid.conf`). This file defines snapshot creation and pruning policies used by `sanoid`.

**Job-Specific Settings (Replace `[JOB_NAME]` with the actual name):**

-   `ZFS_SYNC_JOB_[JOB_NAME]_SOURCE_HOST`: Source host ("local" or hostname/IP) - **Required**
-   `ZFS_SYNC_JOB_[JOB_NAME]_SOURCE_DATASET`: Source ZFS dataset path - **Required**
-   `ZFS_SYNC_JOB_[JOB_NAME]_DEST_HOST`: Destination host ("local" or hostname/IP) - **Required**
-   `ZFS_SYNC_JOB_[JOB_NAME]_DEST_DATASET`: Destination ZFS dataset path - **Required**
-   `ZFS_SYNC_JOB_[JOB_NAME]_SSH_USER`: SSH user for this job (overrides default)
-   `ZFS_SYNC_JOB_[JOB_NAME]_SNAPSHOT_PREFIX`: Snapshot prefix for this job (overrides default)
-   `ZFS_SYNC_JOB_[JOB_NAME]_RECURSIVE`: Recursive setting (`true`/`false`, overrides default)
-   `ZFS_SYNC_JOB_[JOB_NAME]_USE_COMPRESSION`: Compression setting (`true`/`false`, overrides default)
-   `ZFS_SYNC_JOB_[JOB_NAME]_COMPRESSION_METHOD`: `syncoid` compression method (`lz4`, `gzip`, `pigz`, `zstd`, `xz`, `none`, default: `lz4`)
-   `ZFS_SYNC_JOB_[JOB_NAME]_RESUME_SUPPORT`: Resume setting (`true`/`false`, overrides default)
-   `ZFS_SYNC_JOB_[JOB_NAME]_BWLIMIT`: Bandwidth limit for `syncoid` (e.g., `100M` for 100 MiB/s, requires `mbuffer`)

**Important:** Snapshot creation and pruning are primarily controlled by the `sanoid.conf` file specified by `SANOID_CONF_PATH`. The `SNAPSHOT_PREFIX` setting in `.env` is mainly used if the tool needs to create a snapshot itself (which is currently not the default behavior after refactoring).

## Usage

**1. Initialize Submodule:**

If you cloned the repository without submodules, initialize `sanoid`:

```bash
git submodule update --init --recursive
```

**2. Setup & Run:**

Use the provided start script:

```bash
chmod +x start.sh
./start.sh [arguments]
```

This script will:
- Create a Python virtual environment (`.venv`) if it doesn't exist.
- Activate the virtual environment.
- Install dependencies from `requirements.txt`.
- Make `sanoid`/`syncoid` executable.
- Run `zfs_sync.py` with any provided arguments.

**Command-Line Arguments for `zfs_sync.py`:**

-   `-c`, `--config`: Specify path to configuration file (defaults to checking `./.env`).
-   `-j`, `--job`: Run only a specific job name defined in the config. Runs all jobs if omitted.
-   `-i`, `--interactive`: Run in interactive TUI mode (requires `textual`).
-   `--list-jobs`: List job names from config and exit.
-   `--version`: Show version and exit.
-   `--debug`: Enable debug logging.
-   `--dry-run`: Perform a dry run (overrides config setting).

**Example Non-Interactive Run (Specific Job):**

```bash
./start.sh -j mybackupjob
```

**Example Interactive TUI Run:**

```bash
./start.sh -i
```

## Sanoid Configuration (`sanoid.conf`)

This tool relies on `sanoid` for snapshot management. You **must** configure `sanoid.conf` (typically located at `/etc/sanoid/sanoid.conf`, or specify the path via `SANOID_CONF_PATH` in `.env`) to define which datasets `sanoid` should manage and the snapshotting/pruning policies for them.

Refer to the [official sanoid documentation](https://github.com/jimsalterjrs/sanoid) for details on configuring `sanoid.conf`.

## License

This project is open source and available under the MIT License.
