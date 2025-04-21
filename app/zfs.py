import logging
import subprocess
from typing import Optional # Import Optional
from typing import List, Dict # Import typing helpers

from .utils import execute_command # Relative import

# --- ZFS Utilities ---

def has_dataset(dataset: str, host: str, ssh_user: str, config: dict) -> bool:
    """Checks if a ZFS dataset exists."""
    # Dry run check removed - this command should run even in dry run
    logging.debug(f"Checking if dataset '{dataset}' exists on {host}")
    try:
        # Use 'zfs list' which returns 0 if dataset exists, non-zero otherwise
        execute_command(['zfs', 'list', dataset], host=host, ssh_user=ssh_user, config=config,
                        check=True, capture_output=False) # Don't need output, just exit code
        logging.debug(f"Dataset '{dataset}' found on {host}.")
        return True
    except subprocess.CalledProcessError: # Specific exception for non-zero exit when check=True
        logging.debug(f"Dataset '{dataset}' not found on {host}.")
        return False
    except Exception as e: # Catch other potential errors
        logging.error(f"Error checking for dataset '{dataset}' on {host}: {e}")
        return False


def get_snapshot(snapshot_full_name: str, host: str, ssh_user: str, config: dict) -> bool:
    """Checks if a specific ZFS snapshot exists by its full name."""
    # Dry run check removed - this command should run even in dry run
    logging.debug(f"Checking for snapshot '{snapshot_full_name}' on {host}")
    try:
        execute_command(['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', snapshot_full_name],
                        host=host, ssh_user=ssh_user, config=config,
                        check=True, capture_output=False) # Don't need output
        logging.debug(f"Snapshot '{snapshot_full_name}' found on {host}.")
        return True
    except subprocess.CalledProcessError:
        logging.debug(f"Snapshot '{snapshot_full_name}' not found on {host}.")
        return False
    except Exception as e:
        logging.error(f"Error checking for snapshot '{snapshot_full_name}' on {host}: {e}")
        return False


def get_snapshots_with_guids(dataset: str, host: str, ssh_user: str, config: dict) -> dict:
    """Gets a dictionary mapping snapshot names to guids for a dataset."""
    # Dry run check removed - this command should run even in dry run
    logging.debug(f"Getting snapshots and guids for dataset '{dataset}' on {host}")
    snapshots = {}
    try:
        # -p gives parsable output, -r recursive (needed if dataset has children?)
        # -H removes header, -o specifies columns
        cmd_result = execute_command(
            ['zfs', 'list', '-t', 'snapshot', '-o', 'name,guid', '-Hpr', dataset],
            host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True
        )
        output = cmd_result.stdout.strip()
        if not output:
            logging.debug(f"No snapshots found for '{dataset}' on {host}.")
            return snapshots

        for line in output.splitlines():
            try:
                full_name, guid = line.strip().split('\t')
                if '@' in full_name:
                    snap_name = full_name.split('@', 1)[1]
                    snapshots[snap_name] = guid
                    logging.debug(f"Found snapshot on {host}: {snap_name} -> {guid}")
            except ValueError:
                logging.warning(f"Could not parse snapshot line on {host}: {line}")
        return snapshots
    except Exception as e: # Catch potential errors during command execution or parsing
        logging.error(f"Failed to get snapshots for '{dataset}' on {host}: {e}")
        return {} # Return empty dict on failure


# Removed find_verified_common_snapshots (lines 76-105)
# Removed create_snapshot (lines 108-138)
# Removed clean_old_snapshots (lines 141-195)
# Removed setup_sync_snapshot (lines 198-269)
# Removed cleanup_incomplete_snapshots (lines 271-319)
# Removed get_receive_resume_token (lines 321-341)
# Removed estimate_transfer_size (lines 343-406)
def get_datasets(host: str, ssh_user: str, config: dict) -> List[str]:
    """Fetches datasets (filesystems and volumes) from a host."""
    logging.debug(f"Fetching datasets from {host}...")
    try:
        cmd_result = execute_command(
            ['zfs', 'list', '-o', 'name', '-H', '-t', 'filesystem,volume'],
            host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True
        )
        datasets = cmd_result.stdout.strip().splitlines()
        logging.debug(f"Found {len(datasets)} datasets on {host}.")
        # Filter out snapshots just in case (shouldn't be included by -t filesystem,volume but belt-and-suspenders)
        datasets = [ds for ds in datasets if '@' not in ds]
        return datasets
    except Exception as e:
        logging.error(f"Failed to list datasets on {host}: {e}")
        # Re-raise or handle appropriately depending on desired TUI behavior
        raise # Let the caller handle the exception for UI feedback