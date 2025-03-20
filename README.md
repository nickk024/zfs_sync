# ZFS Sync

An intelligent ZFS dataset replication tool for transferring data between systems over the network.

## Features

- Interactive selection of source and destination datasets
- Support for local-to-remote, remote-to-local, local-to-local, and remote-to-remote transfers
- Automatic detection of incremental transfer capabilities
- Creation and management of snapshots on both sides
- Support for recursive datasets (including child datasets)
- Progress indicators with estimated time remaining
- Compression support for faster transfers over slower networks
- Resumable transfers using mbuffer
- Cleanup of old snapshots according to retention policy

## Requirements

- ZFS on both source and destination systems
- SSH key-based authentication for remote systems
- The following utilities:
  - `pv` (for progress visualization)
  - `mbuffer` (optional, for resumable transfers)
  - `pigz` (optional, for faster compression)

## Installation

1. Clone this repository or download the script files:

```bash
git clone https://github.com/yourusername/zfs_sync.git
cd zfs_sync
```

2. Copy the sample environment file and customize it:

```bash
cp .env.sample .env
# Edit the .env file with your preferred settings
```

3. Make the script executable:

```bash
chmod +x zfs_sync.sh
```

## Usage

Simply run the script and follow the interactive prompts:

```bash
./zfs_sync.sh
```

The script will:

1. Check for required dependencies and install them if needed
2. Ask you to select source and destination datasets
3. Verify SSH connectivity to remote hosts
4. Check for existing snapshots on both sides
5. Determine if a full or incremental transfer is needed
6. Create a new snapshot and perform the transfer with progress indication
7. Clean up old snapshots according to your retention policy

## Configuration

You can configure defaults in the `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| DEFAULT_SOURCE_HOST | Hostname of source system (or "local") | "local" |
| DEFAULT_DEST_HOST | Hostname of destination system | "" |
| DEFAULT_SSH_USER | SSH username for remote systems | "root" |
| SNAPSHOT_PREFIX | Prefix for all created snapshots | "backup" |
| MAX_SNAPSHOTS | Number of snapshots to keep | 5 |
| USE_COMPRESSION | Whether to compress data during transfer | true |
| VERIFY_TRANSFERS | Additional verification (slower but more reliable) | false |
| RESUME_SUPPORT | Support for resumable transfers via mbuffer | true |

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
