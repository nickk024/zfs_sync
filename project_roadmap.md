# ZFS Sync Tool Project Roadmap

## Overview
This tool will provide a user-friendly TUI (Text User Interface) to synchronize ZFS datasets between servers using snapshots. It will leverage sanoid for snapshot management and provide features like resumable transfers, progress tracking, and more.

## Target Use Case
- Primary dataset: tank/media (Plex media)
- Source server: Local machine
- Destination: Remote server via SSH

## Features
- [x] Project initialization
- [x] TUI interface
  - [x] Source dataset selection
  - [x] Destination server configuration
  - [x] Transfer options configuration
  - [x] Progress display
  - [x] Log viewer
- [x] Sanoid integration for snapshot management
- [x] ZFS dataset synchronization
  - [x] Initial sync
  - [x] Incremental sync
  - [x] Resumable transfers
- [x] SSH connection management
- [x] Configuration management
  - [x] Save/load configurations
  - [x] Default settings
- [x] Logging and error handling
- [x] Scheduling capabilities
  - [x] Create and manage jobs
  - [x] Schedule jobs via cron
  - [x] List scheduled jobs

## Implementation Plan

### Phase 1: Project Setup ✅
- [x] Create project structure
- [x] Set up virtual environment
- [x] Initialize git repository
- [x] Create basic configuration management

### Phase 2: Core Functionality ✅
- [x] Implement ZFS dataset operations wrapper
- [x] Implement sanoid integration
- [x] Implement SSH connection management
- [x] Create synchronization logic
- [x] Implement resumable transfers

### Phase 3: TUI Development ✅
- [x] Create main TUI framework
- [x] Implement dataset selection interface
- [x] Implement server configuration interface
- [x] Create transfer status and progress display
- [x] Implement log viewer

### Phase 4: Job Management ✅
- [x] Implement job creation and editing
- [x] Implement job execution
- [x] Implement job listing
- [x] Implement job scheduling via cron
- [x] Create comprehensive test script

### Phase 5: Script Consolidation ✅
- [x] Consolidate setup and production scripts into start.sh
- [x] Implement command-line arguments for job management
- [x] Update application code to support job management
- [x] Remove deprecated scripts
- [x] Update documentation

### Phase 6: Testing and Refinement 🔄
- [ ] Test on various dataset sizes
- [ ] Test resumable transfers
- [ ] Test error handling
- [ ] Performance optimization

### Phase 7: Documentation and Deployment 🔄
- [x] Create user documentation
- [x] Create installation guide
- [x] Package for easy deployment
- [x] Create comprehensive test script

### Phase 8: Production Deployment
- [ ] Deploy to production server
- [ ] Configure sanoid on production server
- [ ] Set up initial synchronization
- [ ] Monitor performance and resource usage
- [ ] Implement backup strategy for configuration

### Phase 9: Post-Deployment Testing
- [ ] Verify functionality on production server
- [ ] Test with real-world data volumes
- [ ] Stress test with large datasets
- [ ] Test recovery from interrupted transfers
- [ ] Validate snapshot management

## Technical Considerations
- Python for the main application
- Textual for TUI framework
- Paramiko for SSH operations
- Integration with existing sanoid/syncoid tools
- ZFS command-line tools integration

## Deployment Instructions

### Production Server Setup
1. Ensure the production server has the following prerequisites:
   - Python 3.7 or higher
   - ZFS filesystem
   - SSH server configured
   - Git (for cloning the repository)

2. Clone the repository to the production server:
   ```
   git clone https://github.com/yourusername/zfs_sync.git
   cd zfs_sync
   ```

3. Run the comprehensive test script to verify the environment:
   ```
   chmod +x test.sh
   ./test.sh
   ```

4. Run the setup to initialize the environment:
   ```
   ./start.sh --setup
   ```

5. Configure sanoid:
   - Edit the sanoid configuration file at `~/.zfs_sync/sanoid.conf`
   - Set up appropriate snapshot retention policies
   - Configure datasets to be managed by sanoid

6. Start the application:
   ```
   ./start.sh
   ```

### Job Management
1. Create a job for synchronizing your media:
   ```
   ./start.sh --create-job media_sync
   ```

2. Edit the job to configure:
   ```
   ./start.sh --edit-job media_sync
   ```
   - Source dataset (tank/media)
   - Destination server and dataset
   - Synchronization options

3. Run the job:
   ```
   ./start.sh --run-job media_sync
   ```
   - This may take a long time depending on the dataset size
   - The first sync will be a full backup
   - Subsequent syncs will be incremental

4. Schedule the job for regular execution:
   ```
   ./start.sh --schedule-job media_sync
   ```
   - Enter the cron schedule (e.g., 0 2 * * * for 2:00 AM daily)

5. Verify the scheduled jobs:
   ```
   ./start.sh --list-schedules
   ```

### Ongoing Maintenance
1. Monitor logs for any issues:
   - Check `~/.zfs_sync/logs/` for application logs
   - Review ZFS and sanoid logs for any filesystem issues

2. Perform regular backups of the configuration:
   ```
   cp -r ~/.zfs_sync /path/to/backup/location
   ```

3. Run the test script periodically to verify the system:
   ```
   ./test.sh
   ```

## Future Enhancements
- Web interface option
- Email notifications
- Bandwidth throttling
- Encryption options
- Multiple concurrent transfers
- Automated testing framework
- Performance metrics collection
- Integration with monitoring systems
- Mobile app for remote monitoring
- REST API for remote management
- Support for non-ZFS filesystems
- Differential backup options
- Backup verification and integrity checking
- Disaster recovery automation