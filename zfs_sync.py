#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Import from our library modules
from zfs_sync_lib.config import load_configuration
from zfs_sync_lib.utils import setup_logging, check_prerequisites, verify_ssh
from zfs_sync_lib.zfs import (
    has_dataset, find_verified_common_snapshots, create_snapshot,
    setup_sync_snapshot, clean_old_snapshots, get_snapshot,
    cleanup_incomplete_snapshots # Added missing import
)
from zfs_sync_lib.transfer import perform_full_transfer, perform_incremental_transfer
from zfs_sync_lib.interactive import run_interactive_setup # Import interactive setup

# --- Constants ---
SCRIPT_DIR = Path(__file__).parent.resolve()
DEFAULT_LOG_DIR = SCRIPT_DIR / "logs"
DEFAULT_ENV_FILE = SCRIPT_DIR / ".env"

# --- Job Execution Logic ---
def run_job(job_config: dict, config: dict) -> bool:
    """Runs a single ZFS sync job based on the provided configuration."""
    job_name = job_config.get('_job_name', 'UnknownJob') # Get job name if passed
    run_config = {**config, **job_config} # Combine global and job-specific

    # Extract necessary variables for clarity
    source_host = job_config['source_host']
    source_dataset = job_config['source_dataset']
    dest_host = job_config['dest_host']
    dest_dataset = job_config['dest_dataset']
    ssh_user = job_config['ssh_user']
    recursive = job_config['recursive']
    snapshot_prefix = job_config['snapshot_prefix']
    max_snapshots = job_config['max_snapshots']
    sync_snapshot_name = job_config['sync_snapshot'] # Name like 'backup-sync'

    logging.info("--- Job Configuration ---")
    logging.info(f"  Source:           {ssh_user}@{source_host}:{source_dataset}")
    logging.info(f"  Destination:      {ssh_user}@{dest_host}:{dest_dataset}")
    logging.info(f"  Recursive:        {recursive}")
    logging.info(f"  Snapshot Prefix:  {snapshot_prefix}")
    logging.info(f"  Max Snapshots:    {max_snapshots}")
    logging.info(f"  Sync Snapshot:    {sync_snapshot_name}")
    logging.info(f"  Compression:      {job_config['use_compression']}")
    logging.info(f"  Resume Support:   {job_config['resume_support']}")
    logging.info(f"  Direct Remote:    {job_config['direct_remote_transfer']}")
    logging.info("---")

    # --- Job Logic ---
    try:
        # Verify SSH connections
        verify_ssh(source_host, ssh_user, run_config)
        verify_ssh(dest_host, ssh_user, run_config)

        # Check if destination dataset exists
        logging.info("Verifying datasets...")
        dest_exists = has_dataset(dest_dataset, dest_host, ssh_user, run_config)
        create_if_missing = not dest_exists # If it doesn't exist, we plan to create

        # Confirm source exists
        if not has_dataset(source_dataset, source_host, ssh_user, run_config):
            logging.error(f"Source dataset {source_dataset} does not exist on {source_host}.")
            return False
        logging.info(f"Source dataset confirmed: {source_dataset} on {source_host}")

        if dest_exists:
             logging.info(f"Destination dataset confirmed: {dest_dataset} on {dest_host}")
             # Cleanup potentially incomplete snapshots from previous runs
             cleanup_incomplete_snapshots(dest_dataset, dest_host, ssh_user, run_config, recursive=recursive)
        elif create_if_missing:
             logging.info(f"Destination dataset {dest_dataset} doesn't exist on {dest_host}. Will be created during transfer.")
        else:
             # This state shouldn't be reached with current logic
             logging.error("Destination dataset doesn't exist and auto-creation is somehow disabled.")
             return False

        # Determine transfer type
        logging.info("Checking snapshot status and determining transfer type...")
        needs_initial_transfer = False
        sync_snapshot_src = "" # Base snapshot for incremental

        if not dest_exists:
            logging.info("Destination dataset does not exist. Performing full initial transfer.")
            needs_initial_transfer = True
        else:
            # Destination exists, check for common snapshots to decide transfer type
            # The perform_full_transfer function will handle checking for a resume token internally if needed.
            logging.info("Destination exists. Checking for verified common snapshots...")
            common_snapshots = find_verified_common_snapshots(
                source_dataset, source_host, dest_dataset, dest_host, ssh_user, run_config
            )

            if not common_snapshots:
                logging.warning("No verified common snapshots found between source and destination.")
                # Check if source has *any* snapshots
                try:
                    snap_check_cmd = execute_command(
                        ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', '-d', '1', source_dataset],
                         host=source_host, ssh_user=ssh_user, config=run_config, check=False, capture_output=True
                    )
                    if not snap_check_cmd.stdout.strip():
                         logging.info("Source dataset has no snapshots. Performing full initial transfer.")
                    else:
                         logging.warning("Source has snapshots, but none match destination GUIDs. Performing full initial transfer.")
                except Exception:
                     logging.warning("Could not check source snapshots. Assuming full initial transfer needed.")

                needs_initial_transfer = True
            else:
                # Use the latest verified common snapshot (first in the sorted list)
                sync_snapshot_src = common_snapshots[0]
                logging.info(f"Found verified common snapshot for incremental base: {sync_snapshot_src}")
                needs_initial_transfer = False

        # Create timestamp for new snapshot
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        new_snapshot_name = f"{snapshot_prefix}-{timestamp}"

        # Create source snapshot for transfer
        if not create_snapshot(source_dataset, new_snapshot_name, recursive, source_host, ssh_user, run_config):
             return False # Exit if snapshot creation fails

        # Perform transfer
        logging.info(f"==== Starting ZFS Transfer for Job: {job_name} ====")
        transfer_success = False
        if needs_initial_transfer:
            transfer_success = perform_full_transfer(job_config, config, new_snapshot_name)
            if transfer_success:
                # Create sync snapshot after successful full transfer
                setup_sync_snapshot(source_host, source_dataset, dest_host, dest_dataset,
                                    new_snapshot_name, sync_snapshot_name, ssh_user, recursive, True, run_config)
        else:
            transfer_success = perform_incremental_transfer(job_config, config, new_snapshot_name, sync_snapshot_src)
            if transfer_success:
                 # Update sync snapshot after successful incremental transfer
                 setup_sync_snapshot(source_host, source_dataset, dest_host, dest_dataset,
                                     new_snapshot_name, sync_snapshot_name, ssh_user, recursive, False, run_config)

        if not transfer_success:
            logging.error("ZFS transfer failed.")
            # Optional: attempt to destroy the failed source snapshot?
            # try:
            #     execute_command(['zfs', 'destroy', f'{source_dataset}@{new_snapshot_name}'], host=source_host, ssh_user=ssh_user, config=run_config, check=False)
            # except: pass # Ignore cleanup errors
            return False

        # Clean up old snapshots
        clean_old_snapshots(source_dataset, snapshot_prefix, max_snapshots, source_host, ssh_user, run_config)
        clean_old_snapshots(dest_dataset, snapshot_prefix, max_snapshots, dest_host, ssh_user, run_config)

        # Print summary (optional, could add show_dataset_info equivalent)
        logging.info(f"Job '{job_name}' completed successfully at {datetime.now()}")
        return True

    except Exception as e:
        logging.exception(f"An unexpected error occurred during job '{job_name}': {e}")
        return False


# --- Argument Parsing ---
def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="ZFS Intelligent Replication Script (Python Version)")
    parser.add_argument('--job', action='append', dest='selected_jobs',
                        help='Run only the specified job(s) defined in .env. Can be used multiple times.')
    parser.add_argument('--all-jobs', action='store_true',
                        help='Run all valid jobs defined in ZFS_SYNC_JOB_NAMES in the .env file.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what commands would be executed without actually running them.')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging.')
    parser.add_argument('--env-file', type=Path, default=DEFAULT_ENV_FILE,
                        help=f'Specify path to .env file (default: {DEFAULT_ENV_FILE})')
    parser.add_argument('--log-dir', type=Path, default=DEFAULT_LOG_DIR,
                        help=f'Specify directory for log files (default: {DEFAULT_LOG_DIR})')

    args = parser.parse_args()

    if not args.selected_jobs and not args.all_jobs:
        # If no jobs specified, we'll trigger interactive mode later
        pass

    if args.selected_jobs and args.all_jobs:
        parser.error("Cannot use --job and --all-jobs together.")

    return args

# --- Main Execution ---
def main():
    """Main function."""
    args = parse_arguments()
    # Setup logging first
    setup_logging(args.log_dir, args.debug or os.environ.get('DEBUG_MODE', 'false').lower() == 'true')

    config = load_configuration(args.env_file)
    config['DRY_RUN'] = args.dry_run # Store dry_run status globally
    # Update logging level if debug was set in config file but not CLI
    if not args.debug and config.get('DEBUG_MODE', False):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug mode enabled via config file.")


    # Check prerequisites after loading config (needed for compression check)
    check_prerequisites(config)

    jobs_to_run = []
    if args.all_jobs:
        if not config.get('JOBS'): # Check if JOBS dict exists and is not empty
             logging.error("Option --all-jobs specified, but no valid jobs found in configuration.")
             sys.exit(1)
        jobs_to_run = list(config['JOBS'].keys())
        logging.info(f"Running all configured jobs: {jobs_to_run}")
    elif args.selected_jobs:
        logging.info(f"Running selected jobs: {args.selected_jobs}")
        available_jobs = config.get('JOBS', {})
        for job_name in args.selected_jobs:
            if job_name in available_jobs:
                jobs_to_run.append(job_name)
            else:
                logging.warning(f"Job '{job_name}' specified via --job is not defined or is invalid. Skipping.")
        if not jobs_to_run:
            logging.error("No valid jobs selected to run.")
            sys.exit(1)
    else:
        # Trigger Interactive Mode
        # Pass the run_job function itself so interactive setup can call it
        run_interactive_setup(config, run_job)
        # run_interactive_setup will exit the script

    # --- Run Selected Jobs ---
    overall_success = True
    for job_name in jobs_to_run:
        logging.info(f"================ Starting Job: {job_name} ================")
        job_config = config['JOBS'][job_name]
        job_config['_job_name'] = job_name # Pass job name for logging inside run_job
        success = run_job(job_config, config)
        if not success:
            logging.error(f"!!!!!!!!!!!!!!!! Job '{job_name}' failed. !!!!!!!!!!!!!!!!")
            overall_success = False
            # Decide whether to continue or abort based on a potential config flag?
            # For now, continue with other jobs.
        logging.info(f"================ Finished Job: {job_name} ================")


    logging.info("=================================================")
    if overall_success:
        logging.info("All selected jobs completed successfully.")
        print("All selected jobs completed successfully.")
    else:
        logging.error("One or more jobs failed. Please check the log file.")
        print("One or more jobs failed. Please check the log file.")
        sys.exit(1)

def handle_root_privileges():
    """Checks for root privileges and attempts to re-run with sudo if needed."""
    try:
        if os.geteuid() != 0:
            logging.warning("Script not running as root. Attempting to re-run with sudo...")
            try:
                # Construct the command to re-run the script with sudo
                sudo_cmd = ['sudo', sys.executable] + sys.argv
                logging.info(f"Executing: {' '.join(sudo_cmd)}")
                # Replace the current process with the sudo command
                os.execvp('sudo', sudo_cmd)
                # If execvp returns, it failed
                logging.error("Failed to execute sudo. Please run the script as root or using sudo.")
                sys.exit(1)
            except Exception as e:
                logging.error(f"Failed to re-run with sudo: {e}")
                sys.exit(1)
        else:
            logging.debug("Running with root privileges.")
    except AttributeError: # Handle non-Unix systems like Windows
        logging.warning("Could not check user ID (os.geteuid not available). Assuming privileges are sufficient.")
    except FileNotFoundError: # Handle sudo not being found
         logging.error("'sudo' command not found. Please run the script as root.")
         sys.exit(1)


if __name__ == "__main__":
    # Setup minimal logging first to see potential sudo messages
    # We parse args twice (here and in main) - slightly inefficient but needed
    # to get log/debug flags before the potential sudo exec.
    temp_args = parse_arguments()
    setup_logging(temp_args.log_dir, temp_args.debug or os.environ.get('DEBUG_MODE', 'false').lower() == 'true')

    handle_root_privileges() # Check root and potentially re-execute

    # The rest of the script (main()) will run only in the (potentially new) privileged process.
    main()