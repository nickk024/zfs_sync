# ZFS Sync - ZFS Dataset Replication Tool

A modular shell script utility for reliable ZFS dataset replication between hosts.

## Features

- Interactive selection of source and destination datasets
- Automatically detects if initial full replication is needed
- Creates and manages snapshots on both sides
- Uses incremental sends for efficiency
- Maintains synchronized snapshots for reliable transfers
- Handles recursive datasets
- Shows progress and estimated time remaining
- Cleans up old snapshots according to retention policy
- Robust error handling and verification
- Supports compression and resumable transfers

## Project Structure

The project has been organized into modular components for better maintainability:

```
zfs_sync/
├── lib/                      # Libraries directory
│   ├── config.sh             # Configuration handling
│   ├── logging.sh            # Logging functions
│   ├── utils.sh              # Utility functions
│   ├── datasets.sh           # Dataset and snapshot management
│   ├── transfer.sh           # ZFS transfer operations
│   └── test.sh               # Test functionality
├── zfs_sync.sh               # Main script with modular design
├── test_zfs_sync.sh          # Test script for dry runs
├── .env                      # Environment configuration
└── README.md                 # This file
```

## Prerequisites

- ZFS installed on both source and destination systems
- SSH key-based authentication set up between hosts (if transferring between different hosts)
- Root privileges or sudo access

## Configuration

Copy `.env.sample` to `.env` and modify the settings:

```bash
cp .env.sample .env
nano .env
```

Available settings:

- `DEFAULT_SOURCE_HOST`: Default source host (use "local" for local machine)
- `DEFAULT_DEST_HOST`: Default destination host IP/hostname
- `DEFAULT_SSH_USER`: Default SSH user (typically "root")
- `SNAPSHOT_PREFIX`: Prefix for created snapshots
- `MAX_SNAPSHOTS`: Maximum number of snapshots to keep
- `DEBUG_MODE`: Set to "true" for additional debug output
- `USE_COMPRESSION`: Whether to use compression in transit
- `VERIFY_TRANSFERS`: Verify transfers (slower but more secure)
- `RESUME_SUPPORT`: Enable resumable transfers (requires mbuffer)

## Usage

Run the script with:

```bash
sudo ./zfs_sync.sh
```

The script will guide you through the process:

1. Select source dataset
2. Specify destination host
3. Select destination dataset
4. Choose whether to include child datasets (recursive)
5. Confirm and begin transfer

## Logs

Logs are stored in the `logs/` directory with timestamps.

## Advanced Options

The script automatically selects the optimal transfer method based on your configuration:

- Uses pigz for parallel compression when available
- Falls back to gzip when pigz is not available
- Supports resumable transfers with mbuffer
- Shows progress with pv

## Development and Extending

The modular structure makes it easy to extend or modify the script:

- `config.sh`: Add or modify configuration options
- `logging.sh`: Enhance logging capabilities
- `utils.sh`: Add general utility functions
- `datasets.sh`: Improve dataset management
- `transfer.sh`: Optimize transfer methods

## License

This project is open source and available under the MIT License.
