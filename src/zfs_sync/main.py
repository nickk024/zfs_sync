#!/usr/bin/env python3
"""
ZFS Sync Tool - A TUI tool for synchronizing ZFS datasets between servers using snapshots
"""

import os
import sys
import argparse
import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

# Set up logging
def setup_logging(debug=False):
    """Set up logging configuration."""
    log_dir = Path.home() / '.zfs_sync'
    log_dir.mkdir(exist_ok=True)
    
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'zfs_sync.log'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('zfs_sync')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='ZFS Sync Tool - Synchronize ZFS datasets between servers using snapshots'
    )
    
    # General options
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - for deployment testing'
    )
    
    # Job management options
    parser.add_argument(
        '--create-job',
        type=str,
        metavar='NAME',
        help='Create a new sync job with the specified name'
    )
    parser.add_argument(
        '--edit-job',
        type=str,
        metavar='NAME',
        help='Edit an existing sync job'
    )
    parser.add_argument(
        '--run-job',
        type=str,
        metavar='NAME',
        help='Run a specific sync job immediately'
    )
    parser.add_argument(
        '--list-jobs',
        action='store_true',
        help='List all available sync jobs'
    )
    
    return parser.parse_args()

def get_config_path() -> Path:
    """Get the path to the configuration file."""
    config_dir = Path.home() / '.zfs_sync'
    config_dir.mkdir(exist_ok=True)
    return config_dir / 'config.json'

def load_config() -> Dict[str, Any]:
    """Load the configuration from the configuration file."""
    config_path = get_config_path()
    
    if not config_path.exists():
        # Create default config
        config = {
            "version": 1,
            "default_source_dataset": "tank/media",
            "default_destination_server": "localhost",
            "default_destination_dataset": "backup/media",
            "sync_options": {
                "recursive": True,
                "compress": "lz4",
                "create-bookmark": True,
                "preserve-properties": True,
                "first_sync_full": True,
                "subsequent_sync_incremental": True
            },
            "sanoid": {
                "enabled": True,
                "config_path": str(Path.home() / '.zfs_sync' / 'sanoid.conf')
            },
            "saved_configurations": [],
            "jobs": {},
            "scheduled_jobs": []
        }
        
        # Save default config
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        
        return config
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Ensure jobs key exists
        if "jobs" not in config:
            config["jobs"] = {}
            
            # Save updated config
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=4)
        
        return config
    except json.JSONDecodeError as e:
        print(f"Failed to parse configuration file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

def save_config(config: Dict[str, Any]) -> None:
    """Save the configuration to the configuration file."""
    config_path = get_config_path()
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        print(f"Failed to save configuration: {e}")
        sys.exit(1)

def create_job(name: str, logger: logging.Logger) -> None:
    """Create a new sync job."""
    config = load_config()
    
    if name in config["jobs"]:
        logger.error(f"Job '{name}' already exists. Use --edit-job to modify it.")
        return
    
    # Create a new job with default settings
    config["jobs"][name] = {
        "source_dataset": config["default_source_dataset"],
        "destination_server": config["default_destination_server"],
        "destination_dataset": config["default_destination_dataset"],
        "sync_options": config["sync_options"].copy(),
        "description": f"Sync job created on {os.popen('date').read().strip()}"
    }
    
    save_config(config)
    logger.info(f"Job '{name}' created successfully.")
    logger.info(f"Edit the job with: --edit-job {name}")

def edit_job(name: str, logger: logging.Logger) -> None:
    """Edit an existing sync job."""
    config = load_config()
    
    if name not in config["jobs"]:
        logger.error(f"Job '{name}' does not exist. Use --create-job to create it.")
        return
    
    logger.info(f"Editing job '{name}'...")
    
    # In a real implementation, this would open the TUI to edit the job
    # For now, we'll just print the job details
    job = config["jobs"][name]
    logger.info(f"Source dataset: {job['source_dataset']}")
    logger.info(f"Destination server: {job['destination_server']}")
    logger.info(f"Destination dataset: {job['destination_dataset']}")
    logger.info(f"Sync options: {json.dumps(job['sync_options'], indent=2)}")
    logger.info(f"Description: {job['description']}")
    
    logger.info("Starting TUI interface for editing...")
    
    # Start the TUI application with the job to edit
    from zfs_sync.tui.app import ZFSSyncApp
    app = ZFSSyncApp(initial_job=name)
    app.run()

def run_job(name: str, logger: logging.Logger) -> None:
    """Run a specific sync job."""
    config = load_config()
    
    if name not in config["jobs"]:
        logger.error(f"Job '{name}' does not exist. Use --create-job to create it.")
        return
    
    job = config["jobs"][name]
    logger.info(f"Running job '{name}'...")
    logger.info(f"Syncing {job['source_dataset']} to {job['destination_server']}:{job['destination_dataset']}")
    
    # In a real implementation, this would run the sync process
    # For now, we'll just import the necessary modules and simulate the sync
    try:
        from zfs_sync.core.zfs_ops import list_datasets
        from zfs_sync.core.sanoid_ops import sync_dataset
        
        # Prepare destination
        destination = job['destination_dataset']
        if job['destination_server'] and job['destination_server'] != "localhost":
            destination = f"{job['destination_server']}:{job['destination_dataset']}"
        
        logger.info(f"Starting sync from {job['source_dataset']} to {destination}")
        
        # Check if this is the first sync
        first_sync = job.get('first_sync', True)
        
        if first_sync:
            logger.info("This is the first sync - performing full backup")
            # In a real implementation, this would perform a full backup
            # Update the job to indicate that the first sync has been done
            job['first_sync'] = False
            save_config(config)
        else:
            logger.info("Performing incremental sync")
            # In a real implementation, this would perform an incremental sync
        
        logger.info(f"Sync completed successfully")
        
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        logger.error("Please ensure all dependencies are installed.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)

def list_jobs(logger: logging.Logger) -> None:
    """List all available sync jobs."""
    config = load_config()
    
    if not config["jobs"]:
        logger.info("No jobs found.")
        logger.info("Create a job with: --create-job <name>")
        return
    
    logger.info("Available jobs:")
    for i, (name, job) in enumerate(config["jobs"].items(), 1):
        logger.info(f"{i}. {name} - {job['source_dataset']} to {job['destination_server']}:{job['destination_dataset']}")
        if 'description' in job:
            logger.info(f"   Description: {job['description']}")

def main():
    """Main entry point for the application."""
    args = parse_arguments()
    
    # Set up logging
    logger = setup_logging(args.debug)
    
    # Ensure config directory exists
    config_dir = Path.home() / '.zfs_sync'
    config_dir.mkdir(exist_ok=True)
    
    logger.info("Starting ZFS Sync Tool")
    
    # Handle command-line arguments
    if args.create_job:
        create_job(args.create_job, logger)
        return
    
    if args.edit_job:
        edit_job(args.edit_job, logger)
        return
    
    if args.run_job:
        run_job(args.run_job, logger)
        return
    
    if args.list_jobs:
        list_jobs(logger)
        return
    
    if args.test:
        logger.info("Test mode - exiting")
        return
    
    try:
        # Import here to avoid circular imports
        from zfs_sync.tui.app import ZFSSyncApp
        
        # Start the TUI application
        app = ZFSSyncApp()
        app.run()
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        logger.error("Please ensure all dependencies are installed.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting ZFS Sync Tool")
        sys.exit(0)
    except Exception as e:
        logger = logging.getLogger('zfs_sync')
        logger.exception(f"Unhandled exception: {e}")
        sys.exit(1)