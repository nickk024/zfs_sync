#!/usr/bin/env python3

import argparse
import logging
import sys
import os
import shlex # Import shlex
from datetime import datetime
from typing import Dict, Any, Optional

# Import local library modules
from app.config import load_config, find_config_file, validate_job_config
# Import execute_command directly for sanoid calls
from app.utils import setup_logging, verify_ssh, check_command_exists, execute_command, build_sanoid_command # Import new helper
from app.zfs import (
    # create_snapshot, # Replaced by sanoid --take-snapshots
    # destroy_snapshots_except, # Replaced by sanoid --prune-snapshots
    has_dataset
)
# Import the new unified transfer function
from app.transfer import perform_transfer

# Import the TUI App if available
try:
    from app.tui import ZFSSyncApp
    TEXTUAL_AVAILABLE = True
except ImportError:
    TEXTUAL_AVAILABLE = False
    ZFSSyncApp = None # Placeholder

# Removed old interactive import
# from zfs_sync_lib.interactive import run_interactive_setup

# --- Constants ---
APP_NAME = "zfs-sync"
VERSION = "1.2.0" # Updated version

# --- Helper to run Sanoid ---

def run_sanoid_command(action: str, host: str, ssh_user: Optional[str], run_config: Dict[str, Any]) -> bool:
    """Helper function to execute sanoid commands using the centralized builder."""
    dry_run = run_config.get('DRY_RUN', False) # Still need dry_run for logging logic here

    try:
        cmd = build_sanoid_command(action, run_config)
    except ValueError as e:
        logging.error(str(e))
        return False

    # Determine log action string based on the original action requested
    if action == "take":
        log_action = "Taking snapshots"
    elif action == "prune":
        log_action = "Pruning snapshots"
    else:
        # This case should not be reached if build_sanoid_command raises ValueError
        log_action = f"action '{action}'"

        logging.info(f"[DRY RUN] Would execute sanoid on {host}: {' '.join(shlex.quote(c) for c in cmd)}")
        # Simulate success for dry run, though sanoid might show what it would do
        try:
             # Run with readonly to see potential output, but don't fail job if it errors
             execute_command(cmd, host=host, ssh_user=ssh_user, config=run_config, check=False, capture_output=True)
        except Exception as e:
             logging.warning(f"[DRY RUN] Error simulating sanoid command (continuing): {e}")
        return True # Assume success in dry run

    logging.info(f"Executing sanoid on {host}: {log_action}...")
    try:
        # Use the standard execute_command utility
        result = execute_command(cmd, host=host, ssh_user=ssh_user, config=run_config, check=True, capture_output=True)
        logging.debug(f"Sanoid stdout:\n{result.stdout}")
        if result.stderr:
             logging.warning(f"Sanoid stderr:\n{result.stderr}") # Log stderr as warning
        logging.info(f"Sanoid {log_action} completed successfully on {host}.")
        return True
    except Exception as e:
        # Error already logged by execute_command
        logging.error(f"Sanoid {log_action} failed on {host}.")
        return False


# --- Main Job Processing Logic ---

def run_job(job_config: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """
    Runs a single synchronization job based on the provided configuration using syncoid and sanoid.
    """
    job_name = job_config.get('name', job_config.get('_job_name', 'Unnamed Job'))
    logging.info(f"==== Starting Job: {job_name} ====")

    run_config = {**config, **job_config}
    src_host = run_config['source_host']
    src_dataset = run_config['source_dataset']
    dst_host = run_config['dest_host']
    dst_dataset = run_config['dest_dataset']
    ssh_user = run_config['ssh_user']

    # --- Pre-flight Checks ---
    logging.info("Verifying SSH connection and ZFS on hosts...")
    if not verify_ssh(src_host, ssh_user, run_config): return False
    if src_host != dst_host and not verify_ssh(dst_host, ssh_user, run_config): return False
    logging.info("SSH connection and remote ZFS verified.")

    logging.info("Verifying datasets...")
    src_exists = has_dataset(src_dataset, src_host, ssh_user, run_config)
    if not src_exists:
        logging.error(f"Source dataset '{src_dataset}' not found on host '{src_host}'. Skipping job.")
        return False
    logging.info(f"Source dataset confirmed: {src_dataset} on {src_host}")

    dest_exists = has_dataset(dst_dataset, dst_host, ssh_user, run_config)
    if dest_exists:
        logging.info(f"Destination dataset confirmed: {dst_dataset} on {dst_host}")
    else:
        logging.info(f"Destination dataset {dst_dataset} doesn't exist on {dst_host}. Syncoid will create it.")

    # --- Take Snapshots using Sanoid ---
    if not run_sanoid_command("take", src_host, ssh_user if src_host != "local" else None, run_config):
         logging.error("Failed to take snapshots on source using sanoid. Aborting job.")
         return False

    # --- Perform Transfer using Syncoid ---
    logging.info(f"==== Starting Syncoid Transfer for Job: {job_name} ====")
    # Pass the original job_config dict here, perform_transfer combines it with global config
    transfer_success = perform_transfer(job_config, config, app=None)

    if not transfer_success:
        logging.error("Syncoid transfer failed.")
        # No cleanup needed here as sanoid handles snapshots based on policy
        return False

    logging.info("Syncoid transfer completed successfully.")

    # --- Prune Snapshots using Sanoid (on Destination) ---
    # Pruning should happen *after* successful replication
    if not run_config.get('DRY_RUN', False): # Check dry run before pruning
        if not run_sanoid_command("prune", dst_host, ssh_user if dst_host != "local" else None, run_config):
             logging.warning("Sanoid snapshot pruning failed on destination.")
             # Don't fail the whole job for pruning failure, but log it.
    else:
        logging.info("[DRY RUN] Skipping sanoid prune.")


    logging.info(f"==== Job Completed: {job_name} ====")
    return True


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description=f"{APP_NAME} - ZFS Snapshot Replication Tool v{VERSION}")
    parser.add_argument('-c', '--config', help='Path to configuration file (.env format). Defaults to searching ./.env')
    parser.add_argument('-j', '--job', help='Run only a specific job name defined in the config file.')
    parser.add_argument('-i', '--interactive', action='store_true', help='Run in interactive setup mode (requires textual). This is the default unless --job or --list-jobs is specified.')
    parser.add_argument('--list-jobs', action='store_true', help='List job names defined in the config file and exit.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging.')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run (show commands, no execution). Overrides config setting.')

    args = parser.parse_args()

    # --- Configuration Loading ---
    config_file_path = args.config or find_config_file()
    if not config_file_path:
        print("Error: Configuration file not found.", file=sys.stderr)
        sys.exit(1)

    config = load_config(config_file_path)
    if not config:
        sys.exit(1)

    # --- Logging Setup ---
    log_level = 'DEBUG' if args.debug else config.get('LOG_LEVEL', 'INFO')
    log_file = config.get('LOG_FILE')
    setup_logging(log_level_str=log_level, log_file=log_file, console_logging=not args.interactive)

    logging.info(f"Starting {APP_NAME} v{VERSION}")
    logging.info(f"Using configuration file: {config_file_path}")

    if args.dry_run:
        config['DRY_RUN'] = True
        logging.info("Dry run mode enabled via command line.")

    # --- List Jobs ---
    if args.list_jobs:
        if 'jobs' in config and config['jobs']:
            print("Available jobs:")
            for job_name in config['jobs']:
                print(f"  - {job_name}")
        else:
            print("No jobs defined in the configuration file.")
        sys.exit(0)

    # --- Execute Based on Mode ---
    # Default to interactive mode unless --job or --list-jobs is specified
    if not args.job and not args.list_jobs:
        # --- Interactive Mode ---
        if not TEXTUAL_AVAILABLE:
            # This error should ideally not happen if textual is installed via requirements
            logging.error("Interactive mode requires 'textual' to be installed (`pip install textual`).")
            print("Error: Interactive mode requires 'textual'. Please install it.", file=sys.stderr)
            sys.exit(1)

        logging.info("Starting interactive mode (default)...")
        app = ZFSSyncApp(config=config)
        app.run()
        logging.info("Interactive session finished.")
        print("\nInteractive session finished.")
        sys.exit(0)
    elif args.job:
        # --- Non-Interactive Job Execution ---
        if 'jobs' not in config or not config['jobs']:
            # Check if jobs exist before trying to access the specific one
            logging.error("No jobs defined in the configuration file.")
            sys.exit(1)

        if args.job not in config['jobs']:
            logging.error(f"Job '{args.job}' not found in configuration file.")
            sys.exit(1)

        job_data = config['jobs'][args.job]
        logging.info(f"Running specified job: {args.job}")

        # Validate the specific job
        if not isinstance(job_data, dict):
             logging.warning(f"Job '{args.job}' has invalid configuration (not a dictionary). Skipping.")
             sys.exit(1)

        temp_job_config = {'name': args.job, **job_data}
        if not validate_job_config(temp_job_config):
            logging.warning(f"Job '{args.job}' has invalid configuration. Skipping.")
            sys.exit(1)

        # Run the single job
        job_success = run_job(job_data, config)
        if not job_success:
            logging.error(f"Job '{args.job}' failed.")

        logging.info(f"Job '{args.job}' processed.")
        sys.exit(0 if job_success else 1)
    # Note: The case for args.list_jobs is handled earlier by exiting.
    # If neither interactive nor --job is specified, something is wrong.
    else:
         logging.error("Internal logic error: Should have run interactive or a specific job.")
         sys.exit(1)


if __name__ == "__main__":
    main()