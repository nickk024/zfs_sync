# ZFS Sync Tool

A TUI (Text User Interface) tool for synchronizing ZFS datasets between servers using snapshots. This tool is designed to make it easy to keep your ZFS datasets in sync, with a focus on Plex media libraries.

## Features

- Interactive TUI interface for easy configuration and monitoring
- Integration with sanoid for snapshot management
- Support for resuming interrupted transfers
- Configurable synchronization options
- Save and load configurations for different sync scenarios
- Progress monitoring and logging
- SSH connection management for remote servers

## Requirements

- Python 3.7 or higher
- ZFS filesystem
- sanoid/syncoid (included in the libs directory)
- SSH access to remote servers (if syncing to remote servers)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/zfs_sync.git
   cd zfs_sync
   ```

2. Make the start script executable:
   ```
   chmod +x start.sh
   ```

3. Run the setup:
   ```
   ./start.sh --setup
   ```

   This will:
   - Create a virtual environment
   - Install required dependencies
   - Initialize a git repository
   - Create default configurations

## Usage

### Testing

Before using the tool, you can run the comprehensive test script to verify that everything is set up correctly:

```bash
./test.sh
```

This will:
- Check prerequisites
- Set up the environment
- Test job management
- Verify the application functionality
- Write detailed logs to `~/.zfs_sync/logs/`

### Basic Usage

1. Start the application:
   ```
   ./start.sh
   ```

2. Use the TUI to:
   - Select the source dataset
   - Configure the destination server
   - Select the destination dataset
   - Configure synchronization options
   - Start the synchronization process
   - Monitor progress

### Job Management

The tool supports creating and managing sync jobs:

```bash
# Create a new job
./start.sh --create-job media_backup

# Edit an existing job
./start.sh --edit-job media_backup

# Run a job
./start.sh --run-job media_backup

# List all jobs
./start.sh --list-jobs

# Schedule a job via cron
./start.sh --schedule-job media_backup

# View scheduled jobs
./start.sh --list-schedules
```

### First-time Setup

If you're setting up the tool for the first time:

```bash
# Run the setup
./start.sh --setup
```

This will:
- Create a virtual environment
- Install required dependencies
- Initialize a git repository
- Create default configurations

### Keyboard Shortcuts

- `s`: Start synchronization
- `c`: Save configuration
- `r`: Refresh
- `q`: Quit
- `d`: Toggle dark mode
- `h`: Show help

## Configuration

The application stores its configuration in `~/.zfs_sync/config.json`. This includes:

- Default source dataset
- Default destination server
- Default destination dataset
- Synchronization options
- Saved configurations

## Sanoid Integration

This tool integrates with sanoid for snapshot management. Sanoid is included in the `libs/sanoid` directory.

To use sanoid:

1. Configure sanoid in `~/.zfs_sync/sanoid.conf`
2. Enable sanoid integration in the application settings

## Development

### Project Structure

```
zfs_sync/
├── libs/
│   └── sanoid/       # Sanoid/syncoid tools
├── src/
│   └── zfs_sync/
│       ├── core/     # Core functionality
│       ├── tui/      # TUI interface
│       └── main.py   # Entry point
├── start.sh          # Main script for setup and running
├── test.sh           # Comprehensive test script
└── README.md         # This file
```

### Core Modules

- `zfs_ops.py`: ZFS operations
- `ssh_ops.py`: SSH operations
- `sanoid_ops.py`: Sanoid integration
- `config_manager.py`: Configuration management

### TUI Modules

- `app.py`: Main TUI application
- `screens/`: Screen definitions
- `widgets/`: Custom widgets

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GPL v3 License - see the LICENSE file for details.

## Acknowledgments

- [sanoid/syncoid](https://github.com/jimsalterjrs/sanoid) for ZFS snapshot management
- [Textual](https://github.com/Textualize/textual) for the TUI framework