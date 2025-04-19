import logging
import subprocess
from typing import Optional # Import Optional
from .utils import execute_command # Relative import

# --- ZFS Utilities ---

def has_dataset(dataset: str, host: str, ssh_user: str, config: dict) -> bool:
    """Checks if a ZFS dataset exists."""
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


def find_verified_common_snapshots(src_dataset: str, src_host: str, dst_dataset: str, dst_host: str, ssh_user: str, config: dict) -> list:
    """Finds snapshots common to source and destination by comparing name and guid."""
    logging.info("Checking for verified common snapshots (matching name and guid)...")
    src_snaps = get_snapshots_with_guids(src_dataset, src_host, ssh_user, config)
    dst_snaps = get_snapshots_with_guids(dst_dataset, dst_host, ssh_user, config)

    common_verified_snaps = []
    for snap_name, src_guid in src_snaps.items():
        if snap_name in dst_snaps and dst_snaps[snap_name] == src_guid:
            logging.debug(f"Found verified common snapshot: {snap_name} (GUID: {src_guid})")
            common_verified_snaps.append(snap_name)
        elif snap_name in dst_snaps:
            logging.debug(f"Snapshot '{snap_name}' exists on both, but GUIDs differ (Src: {src_guid}, Dst: {dst_snaps[snap_name]}). Not common.")

    # Sort by name (often includes timestamp) - reverse for newest first
    common_verified_snaps.sort(reverse=True)

    if not common_verified_snaps:
        logging.info("No verified common snapshots found.")
    else:
        logging.info(f"Found {len(common_verified_snaps)} verified common snapshots.")

    return common_verified_snaps


def create_snapshot(dataset: str, snapshot_name: str, recursive: bool, host: str, ssh_user: str, config: dict) -> bool:
    """Creates a ZFS snapshot."""
    full_snapshot_name = f"{dataset}@{snapshot_name}"
    if get_snapshot(full_snapshot_name, host, ssh_user, config):
        logging.warning(f"Snapshot '{full_snapshot_name}' already exists on {host}. Skipping creation.")
        return True

    logging.info(f"Creating snapshot '{full_snapshot_name}' on {host}")
    cmd = ['zfs', 'snapshot']
    if recursive:
        cmd.append('-r')
    cmd.append(full_snapshot_name)

    try:
        execute_command(cmd, host=host, ssh_user=ssh_user, config=config, check=True)
        # Verify creation only if not in dry run mode
        if not config.get('DRY_RUN', False):
            if not get_snapshot(full_snapshot_name, host, ssh_user, config):
                 logging.error(f"Verification failed: Snapshot '{full_snapshot_name}' not found after creation attempt on {host}.")
                 return False
        logging.info(f"Successfully created and verified snapshot '{full_snapshot_name}' on {host}")
        return True
    except Exception as e:
        # Error logged by execute_command
        # logging.error(f"Failed to create snapshot '{full_snapshot_name}' on {host}: {e}")
        return False


def clean_old_snapshots(dataset: str, prefix: str, keep: int, host: str, ssh_user: str, config: dict):
    """Cleans up old snapshots matching a prefix, keeping the newest N."""
    logging.info(f"Cleaning old snapshots with prefix '{prefix}' on {host}:{dataset} (keeping newest {keep})...")
    try:
        # Get all snapshots for the dataset, sorted reverse (newest first)
        cmd_result = execute_command(
            ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-s', 'creation', '-r', '-H', dataset],
             host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True
        )
        all_snapshots = cmd_result.stdout.strip().splitlines()
    except Exception as e:
        logging.error(f"Failed to list snapshots for cleanup on {host}:{dataset}: {e}")
        return

    # Filter by prefix
    prefix_snapshots = []
    full_prefix = f"{dataset}@{prefix}"
    for snap_full_name in all_snapshots:
        # Ensure it starts with the prefix and has something after it
        if snap_full_name.startswith(full_prefix) and len(snap_full_name) > len(full_prefix):
            prefix_snapshots.append(snap_full_name)

    logging.debug(f"Found {len(prefix_snapshots)} snapshots with prefix '{prefix}' for dataset {dataset} on {host}")

    if len(prefix_snapshots) <= keep:
        logging.info(f"Found {len(prefix_snapshots)} snapshots <= {keep} to keep. No snapshots will be removed.")
        return

    to_remove_count = len(prefix_snapshots) - keep
    snapshots_to_remove = prefix_snapshots[keep:] # The oldest ones are at the end due to reverse sort

    logging.info(f"Will remove {to_remove_count} oldest snapshots:")
    removed_count = 0
    failed_count = 0
    for snap_to_remove in snapshots_to_remove:
        logging.debug(f"Attempting to remove snapshot: {snap_to_remove}")
        try:
            # Assuming recursive destroy is usually desired for snapshots matching a prefix
            execute_command(['zfs', 'destroy', '-r', snap_to_remove],
                            host=host, ssh_user=ssh_user, config=config, check=True)
            removed_count += 1
        except Exception:
            failed_count += 1
            # Error is logged by execute_command

    logging.info(f"Snapshot cleanup summary: Removed {removed_count}, Failed {failed_count}")


def setup_sync_snapshot(src_host: str, src_dataset: str, dst_host: str, dst_dataset: str,
                        current_snapshot: str, sync_snapshot_name: str, ssh_user: str,
                        recursive: bool, create_new: bool, config: dict) -> bool:
    """Sets up or updates the sync snapshot on source and destination."""
    src_sync_snap_full = f"{src_dataset}@{sync_snapshot_name}"
    dst_sync_snap_full = f"{dst_dataset}@{sync_snapshot_name}"
    src_current_snap_full = f"{src_dataset}@{current_snapshot}"
    dst_current_snap_full = f"{dst_dataset}@{current_snapshot}"

    logging.info("Setting up common sync snapshot on both hosts...")

    # --- Source Side ---
    logging.debug(f"Processing source sync snapshot: {src_sync_snap_full}")
    if get_snapshot(src_sync_snap_full, src_host, ssh_user, config):
        logging.info(f"Removing existing sync snapshot on source: {src_sync_snap_full}")
        try:
            # Don't exit script if destroy fails, just warn
            execute_command(['zfs', 'destroy', '-r', src_sync_snap_full],
                            host=src_host, ssh_user=ssh_user, config=config, check=False)
        except Exception as e:
             logging.warning(f"Could not destroy old sync snapshot on source: {src_sync_snap_full} - {e}")

    if create_new:
        logging.info(f"Creating new sync snapshot on source: {src_sync_snap_full}")
        if not create_snapshot(src_dataset, sync_snapshot_name, recursive, src_host, ssh_user, config):
             return False # Propagate failure
    else:
        logging.info(f"Renaming current snapshot '{src_current_snap_full}' to sync snapshot '{src_sync_snap_full}' on source")
        try:
            execute_command(['zfs', 'rename', src_current_snap_full, src_sync_snap_full],
                            host=src_host, ssh_user=ssh_user, config=config, check=True)
        except Exception as e:
             # Error logged by execute_command
             logging.error(f"Failed to rename to sync snapshot on source.")
             return False # Indicate failure

    # --- Destination Side ---
    logging.debug(f"Processing destination sync snapshot: {dst_sync_snap_full}")
    if get_snapshot(dst_sync_snap_full, dst_host, ssh_user, config):
        logging.info(f"Removing existing sync snapshot on destination: {dst_sync_snap_full}")
        try:
            execute_command(['zfs', 'destroy', '-r', dst_sync_snap_full],
                            host=dst_host, ssh_user=ssh_user, config=config, check=False)
        except Exception as e:
             logging.warning(f"Could not destroy old sync snapshot on destination: {dst_sync_snap_full} - {e}")

    if create_new:
        logging.info(f"Creating new sync snapshot on destination: {dst_sync_snap_full}")
        if not create_snapshot(dst_dataset, sync_snapshot_name, recursive, dst_host, ssh_user, config):
            return False # Propagate failure
    else:
        # Need the *current* snapshot name on the destination, which should match the source's current
        logging.info(f"Renaming current snapshot '{dst_current_snap_full}' to sync snapshot '{dst_sync_snap_full}' on destination")
        try:
             execute_command(['zfs', 'rename', dst_current_snap_full, dst_sync_snap_full],
                             host=dst_host, ssh_user=ssh_user, config=config, check=True)
        except Exception as e:
             # Error logged by execute_command
             logging.error(f"Failed to rename to sync snapshot on destination.")
             return False # Indicate failure

    # --- Verification (skip in dry run) ---
    if not config.get('DRY_RUN', False):
        if not get_snapshot(src_sync_snap_full, src_host, ssh_user, config):
            logging.error("Failed to verify sync snapshot on source after setup!")
            return False
        if not get_snapshot(dst_sync_snap_full, dst_host, ssh_user, config):
            logging.error("Failed to verify sync snapshot on destination after setup!")
            return False

    logging.info("Sync snapshots successfully set up on both source and destination.")
    return True

def cleanup_incomplete_snapshots(dataset: str, host: str, ssh_user: str, config: dict,
                                 pattern: str = "_zfs_sync_incomplete_", recursive: bool = True):
    """Finds and removes snapshots matching a specific pattern, often used for cleanup."""
    # Note: The original bash script used grep -F for fixed string matching.
    # Here, we list all and filter in Python for simplicity, though less efficient.
    # A pattern like f"{dataset}@{pattern}" could be used with zfs list directly if needed.
    logging.info(f"Checking for potentially incomplete snapshots on {host}:{dataset} matching pattern '@{pattern}*'")
    try:
        cmd_result = execute_command(
            ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', '-r', dataset], # List recursively
             host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True
        )
        all_snapshots = cmd_result.stdout.strip().splitlines()
    except Exception as e:
        logging.error(f"Failed to list snapshots for incomplete cleanup on {host}:{dataset}: {e}")
        return

    snapshots_to_remove = []
    pattern_full_prefix = f"{dataset}@{pattern}"
    for snap_full_name in all_snapshots:
        # Check if the snapshot name itself starts with the pattern after the dataset@ part
        if '@' in snap_full_name:
             base_dataset, snap_name_part = snap_full_name.split('@', 1)
             # Check if the base dataset matches AND the snapshot part starts with the pattern
             if base_dataset == dataset and snap_name_part.startswith(pattern):
                  snapshots_to_remove.append(snap_full_name)

    if not snapshots_to_remove:
        logging.debug("No incomplete snapshots found matching the pattern.")
        return

    logging.info(f"Found {len(snapshots_to_remove)} potentially incomplete snapshots to remove:")
    removed_count = 0
    failed_count = 0
    for snap_to_remove in snapshots_to_remove:
        logging.debug(f"Attempting to remove incomplete snapshot: {snap_to_remove}")
        try:
            cmd = ['zfs', 'destroy']
            # Use recursive destroy matching the original script's behavior for this cleanup
            if recursive: cmd.append('-r')
            cmd.append(snap_to_remove)
            execute_command(cmd, host=host, ssh_user=ssh_user, config=config, check=True)
            removed_count += 1
            logging.info(f"Removed incomplete snapshot: {snap_to_remove}")
        except Exception:
            failed_count += 1
            # Error is logged by execute_command

    logging.info(f"Incomplete snapshot cleanup summary: Removed {removed_count}, Failed {failed_count}")

def estimate_transfer_size(dataset: str, host: str, ssh_user: str, config: dict,
                           base_snapshot: Optional[str] = None, new_snapshot: Optional[str] = None) -> Optional[int]:
    """Estimates the size of a ZFS send operation in bytes."""
    if base_snapshot and new_snapshot:
        # Estimate incremental size
        logging.info(f"Estimating incremental send size for {dataset}@{base_snapshot} -> {new_snapshot} on {host}...")
        cmd = ['zfs', 'send', '-nvP', '-i', f"{dataset}@{base_snapshot}", f"{dataset}@{new_snapshot}"]
        try:
            result = execute_command(cmd, host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True)
            # Parse output like: "size 123456" or "total estimated size is 123456 bytes"
            size_bytes = None
            output = result.stderr or result.stdout # -n sends verbose output to stdout, -v to stderr
            for line in output.strip().splitlines():
                 if "total estimated size is" in line:
                     parts = line.split()
                     if len(parts) >= 5 and parts[-1] == 'bytes':
                         size_bytes = int(parts[-2])
                         break
                 elif "size" in line and len(line.split()) == 2: # Fallback for simpler output format
                      try:
                          size_bytes = int(line.split()[1])
                          break
                      except ValueError:
                          continue
            if size_bytes is not None:
                 logging.info(f"Estimated incremental size: {size_bytes} bytes")
                 return size_bytes
            else:
                 logging.warning(f"Could not parse estimated size from zfs send output:\n{output}")
                 return None # Indicate failure to parse
        except Exception as e:
            logging.error(f"Failed to estimate incremental send size: {e}")
            return None
    elif new_snapshot:
        # Estimate full size (use 'used' property of the snapshot)
        logging.info(f"Estimating full send size for {dataset}@{new_snapshot} on {host}...")
        full_snap_name = f"{dataset}@{new_snapshot}"
        cmd = ['zfs', 'list', '-o', 'used', '-Hp', '-t', 'snapshot', full_snap_name]
        try:
            result = execute_command(cmd, host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True)
            size_bytes = int(result.stdout.strip())
            logging.info(f"Estimated full size (snapshot used): {size_bytes} bytes")
            return size_bytes
        except Exception as e:
            logging.error(f"Failed to estimate full send size using snapshot 'used' property: {e}")
            # Fallback to dataset used size? Might be inaccurate.
            logging.info(f"Falling back to estimating full send size using dataset 'used' property for {dataset} on {host}...")
            cmd = ['zfs', 'list', '-o', 'used', '-Hp', '-d', '0', dataset] # -d 0 for only the dataset itself
            try:
                result = execute_command(cmd, host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True)
                size_bytes = int(result.stdout.strip())
                logging.info(f"Estimated full size (dataset used): {size_bytes} bytes")
                return size_bytes
            except Exception as e2:
                 logging.error(f"Failed to estimate full send size using dataset 'used' property: {e2}")
                 return None
    else:
        logging.error("Cannot estimate size without at least a new_snapshot name.")
        return None