import logging
import subprocess
import threading
import select
import re
import shlex
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, TYPE_CHECKING

# Import necessary functions from other modules within the 'lib' package
from .utils import execute_command, check_command_exists
# Keep zfs utils for now, might remove later
# estimate_transfer_size, get_receive_resume_token, has_dataset are no longer needed here
# as syncoid handles resume and size estimation, and dataset checks happen before calling perform_transfer.

# Type hint for the TUI screen without causing circular import
if TYPE_CHECKING:
    # Import the specific screen class that provides the necessary methods
    from .tui.screens.transfer import TransferScreen # Corrected import path

# --- Stderr Processing Thread ---

def _parse_zfs_size(size_str: str) -> int:
    """Parses ZFS size string (e.g., 1.23G, 45M, 100K) into bytes."""
    size_str = size_str.upper().strip() # Ensure clean string
    if not size_str: return 0

    unit = 'B' # Default unit bytes
    unit_multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4, 'P': 1024**5}

    # Check last character for unit
    last_char = size_str[-1]
    if last_char in unit_multipliers:
        unit = last_char
        size_str = size_str[:-1]
    elif last_char == 'B' and len(size_str) > 1: # Handle trailing 'B' like '100KB'
        size_str = size_str[:-1]
        last_char = size_str[-1]
        if last_char in unit_multipliers:
             unit = last_char
             size_str = size_str[:-1]

    try:
        size = float(size_str)
        if unit != 'B':
            size *= unit_multipliers[unit]
        return int(size)
    except ValueError:
        # Log to standard logging, not TUI
        logging.warning(f"Could not parse ZFS size string: '{size_str}' (original unit '{unit}')")
        return 0

# TODO: Refactor this function to parse `syncoid --progress` output
# Note: The 'app' parameter here is actually the TransferScreen instance
def _process_zfs_stderr(stderr_pipe, app: 'TransferScreen'):
    """
    Reads syncoid stderr, parses progress, and updates the Textual TUI via the screen instance.
    Runs in a separate thread.
    *** THIS NEEDS REFACTORING FOR SYNCOID ***
    """
    # Regex to find size information in zfs send -v output
    # Matches lines like: "10:26:20   16.4M   tank/media@initial-sync"
    # Regex for syncoid --progress output
    # Example: INFO: Sending incremental ... (~ 1.23 GiB): 50% | 615 MiB / 1.2 GiB | 100 MiB/s | 00:00:06 ETA
    # Captures: Percentage, Completed Size Str, Total Size Str
    syncoid_progress_pattern = re.compile(
        r":\s*(\d+)%\s*\|\s*([\d.]+\s*[KMGTP]i?B)\s*/\s*([\d.]+\s*[KMGTP]i?B)", re.IGNORECASE
    )
    total_bytes_reported = 0 # Keep track of the last reported absolute byte count

    # Use select for non-blocking reads
    poller = select.poll()
    poller.register(stderr_pipe, select.POLLIN)

    while True:
        # Wait for data, but with a timeout to allow checking thread status
        if poller.poll(100): # 100ms timeout
            try:
                line_bytes = stderr_pipe.readline() # Read bytes
                if not line_bytes: # EOF
                    break
                line = line_bytes.decode('utf-8', errors='replace').strip() # Decode safely
            except OSError: # Handle pipe closed errors
                 break

            # logging.debug(f"Syncoid Stderr Raw: {line}")

            # --- Parse syncoid --progress output ---
            progress_match = syncoid_progress_pattern.search(line)
            if progress_match:
                try:
                    # percentage = int(progress_match.group(1)) # Percentage is useful but sizes are more direct
                    completed_str = progress_match.group(2)
                    total_str = progress_match.group(3)

                    current_completed_bytes = _parse_zfs_size(completed_str)
                    current_total_bytes = _parse_zfs_size(total_str)

                    # Update TUI using the parsed byte counts
                    # Only update if completed bytes have increased to avoid redundant updates
                    if current_completed_bytes > total_bytes_reported:
                        app.update_progress(completed=current_completed_bytes, total=current_total_bytes)
                        total_bytes_reported = current_completed_bytes # Update last reported absolute value

                    # Log the raw progress line for debugging if needed
                    # app.log_message(f"stderr: {line}", level=logging.DEBUG)

                except (IndexError, ValueError) as e:
                    app.log_message(f"Error parsing syncoid progress line: '{line}' - {e}", level=logging.WARNING)
                except Exception as e: # Catch any other unexpected errors during parsing
                    app.log_message(f"Unexpected error parsing progress: '{line}' - {e}", level=logging.ERROR)
                    logging.exception(f"Unexpected error parsing progress line: {line}")


            elif line and "INFO:" not in line and "WARNING:" not in line:
                 # Log other potentially relevant stderr lines that aren't standard INFO/WARN
                 # Avoid logging the basic INFO/WARNING lines unless debugging is high
                 app.log_message(f"stderr: {line}", level=logging.DEBUG)

        else:
            # Check if pipe is closed from the other end
            if stderr_pipe.closed:
                 break
            # Poller timeout handles waiting

    app.log_message("Stderr processing thread finished.", level=logging.DEBUG)
    # Ensure progress reaches 100% in TUI if the total was determined
    # Use the app's reactive variables as the source of truth
    final_total = app.total_bytes
    final_completed = app.completed_bytes
    if final_total is not None and final_total > 0:
        if final_completed < final_total:
            app.log_message(f"Transfer finished but reported bytes ({final_completed}) < total ({final_total}). Setting to 100%.", level=logging.WARNING)
            app.update_progress(completed=final_total, total=final_total)
        else:
            # Ensure it's exactly at total if slightly over or already there
             app.update_progress(completed=final_total, total=final_total)
    elif total_bytes_reported > 0: # If size was unknown but we reported some progress
         app.update_progress(completed=total_bytes_reported, total=total_bytes_reported)


# --- Simplified Execution (Replaces execute_transfer_pipeline) ---
# We might need a more robust way to handle syncoid's output/progress later

def execute_syncoid_transfer(syncoid_cmd: List[str], config: dict, app: Optional['TransferScreen']) -> bool:
    """
    Executes a single syncoid command, capturing stderr for TUI progress.
    """
    is_tui_run = app is not None
    log_func = app.log_message if is_tui_run else logging.info
    dry_run = config.get('dry_run', config.get('DRY_RUN', False))

    # Add --progress flag if in TUI mode for updates
    if is_tui_run and '--progress' not in syncoid_cmd:
        syncoid_cmd.append('--progress')

    log_cmd_str = ' '.join(shlex.quote(arg) for arg in syncoid_cmd)

    if dry_run:
        log_func(f"[DRY RUN] Planned syncoid command: {log_cmd_str}")
        # Attempt to parse size from dry run output (syncoid -n)
        try:
            # Ensure -n is present for dry run size estimation
            if '-n' not in syncoid_cmd:
                syncoid_cmd.insert(1, '-n') # Add -n after 'syncoid'
            log_cmd_str_dry = ' '.join(shlex.quote(arg) for arg in syncoid_cmd)
            log_func(f"[DRY RUN] Executing for size estimate: {log_cmd_str_dry}", level=logging.DEBUG)
            # Use execute_command utility for dry run to capture output
            result = execute_command(syncoid_cmd, host="local", ssh_user=None, config=config, check=True, capture_output=True)
            output = result.stdout + result.stderr # Check both streams
            # Example syncoid -n output line: "Will replicate ~ 1.23 GiB"
            size_match = re.search(r"Will replicate ~ ([\d.]+ [KMGTP]i?B)", output, re.IGNORECASE)
            if size_match:
                size_str = size_match.group(1).replace('i', '') # Remove 'i' from GiB etc.
                estimated_size = _parse_zfs_size(size_str)
                log_func(f"[DRY RUN] Estimated transfer size: {size_str} ({estimated_size} bytes)")
                if is_tui_run:
                    app.update_progress(completed=estimated_size, total=estimated_size, action="Dry Run Complete")
            else:
                log_func("[DRY RUN] Could not parse estimated size from syncoid -n output.", level=logging.WARNING)
                if is_tui_run:
                    app.update_progress(completed=1, total=1, action="Dry Run Complete (Size Unknown)")

        except Exception as e:
            log_func(f"[DRY RUN] Failed to estimate size using syncoid -n: {e}", level=logging.ERROR)
            if is_tui_run:
                app.update_progress(completed=1, total=1, action="Dry Run Failed")
            return False # Indicate failure even in dry run if estimation fails
        return True # Dry run simulated success

    # --- Actual Syncoid Execution ---
    log_func(f"Executing: {log_cmd_str}", level=logging.INFO)
    process = None
    stderr_thread = None
    stderr_pipe_read_end = None
    success = False

    try:
        # Use Popen to manage the process and capture stderr for the thread
        process = subprocess.Popen(syncoid_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0) # Unbuffered stderr

        if is_tui_run:
            stderr_pipe_read_end = process.stderr
            # Start the stderr processing thread (needs refactoring for syncoid)
            stderr_thread = threading.Thread(target=_process_zfs_stderr, args=(stderr_pipe_read_end, app), daemon=True)
            stderr_thread.start()

        # Capture stdout for logging after completion
        stdout_bytes, _ = process.communicate() # stderr is handled by the thread
        stdout_output = stdout_bytes.decode('utf-8', errors='replace').strip()

        # Wait for process completion handled by communicate()
        return_code = process.returncode

        # Wait for the stderr processing thread to finish
        if stderr_thread:
            if stderr_pipe_read_end and not stderr_pipe_read_end.closed:
                 try: stderr_pipe_read_end.close()
                 except: pass
            stderr_thread.join(timeout=5)
            if stderr_thread.is_alive():
                 log_func("Stderr processing thread did not finish cleanly.", level=logging.WARNING)

        if return_code == 0:
            log_func("Syncoid command completed successfully.", level=logging.INFO)
            if stdout_output:
                log_func(f"Syncoid stdout:\n{stdout_output}", level=logging.DEBUG)
            success = True
        else:
            log_func(f"Syncoid command failed with exit code {return_code}.", level=logging.ERROR)
            # Stderr should have been logged by the thread or captured if not TUI
            if stdout_output: # Log stdout on failure too
                log_func(f"Syncoid stdout:\n{stdout_output}", level=logging.ERROR)
            success = False

    except FileNotFoundError:
        log_func(f"Error: '{syncoid_cmd[0]}' command not found. Is sanoid/syncoid installed and in PATH or libs/sanoid?", level=logging.CRITICAL)
        success = False
    except Exception as e:
        log_func(f"Error executing syncoid: {e}", level=logging.ERROR)
        logging.exception("Syncoid execution error") # Log traceback
        success = False
    finally:
        # Ensure process is terminated if it's still running (shouldn't be after communicate)
        if process and process.poll() is None:
            log_func(f"Terminating syncoid process PID {process.pid}", level=logging.WARNING)
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                log_func(f"Killing syncoid process PID {process.pid}", level=logging.WARNING)
                process.kill()
            except Exception as kill_e:
                log_func(f"Error terminating/killing syncoid process {process.pid}: {kill_e}", level=logging.ERROR)
        # Ensure pipes are closed
        try:
            if process and process.stdout: process.stdout.close()
            # stderr pipe is closed by the thread or above
        except: pass

    return success


# --- Transfer Functions (Refactored) ---

def get_compression_commands(config: dict) -> Optional[str]:
    """Gets the appropriate syncoid compression argument."""
    # Use job-specific or default compression setting
    use_compression = config.get('use_compression', config.get('DEFAULT_USE_COMPRESSION', False))
    compress_method = config.get('compression_method', 'lz4') # Default to lz4 if unspecified

    if use_compression:
        # Basic check if method is somewhat valid (syncoid validates fully)
        if compress_method in ['lz4', 'gzip', 'pigz', 'zstd', 'none', 'xz']:
             logging.debug(f"Using syncoid compression: {compress_method}")
             return compress_method
        else:
             logging.warning(f"Unsupported compression method '{compress_method}' specified for syncoid. Defaulting to lz4.")
             return 'lz4' # Default to lz4 on invalid input
    return None # No compression argument


# Note: The 'app' parameter here is actually the TransferScreen instance (or None for non-TUI runs)
def perform_transfer(job_config: dict, config: dict, app: Optional['TransferScreen']) -> bool:
    """
    Performs a ZFS transfer (full or incremental) using syncoid.
    This replaces both perform_full_transfer and perform_incremental_transfer.
    """
    is_tui_run = app is not None
    log_func = app.log_message if is_tui_run else logging.info

    src_host = job_config['source_host']
    src_dataset = job_config['source_dataset']
    dst_host = job_config['dest_host']
    dst_dataset = job_config['dest_dataset']
    ssh_user = job_config['ssh_user']
    recursive = job_config['recursive']
    run_config = {**config, **job_config}
    dry_run = run_config.get('dry_run', run_config.get('DRY_RUN', False))

    # --- Construct syncoid Source and Destination Arguments ---
    # Handle local vs remote paths
    source_arg = f"{ssh_user}@{src_host}:{src_dataset}" if src_host != "local" else src_dataset
    dest_arg = f"{ssh_user}@{dst_host}:{dst_dataset}" if dst_host != "local" else dst_dataset

    # --- Build syncoid Command ---
    # Use the syncoid script from the submodule path
    syncoid_executable = run_config.get('SYNCOID_PATH', 'libs/sanoid/syncoid') # Allow overriding path via config
    syncoid_cmd = [syncoid_executable]

    # Add common options based on job/global config
    if recursive:
        syncoid_cmd.append('-r')

    # Compression
    compress_method = get_compression_commands(run_config)
    if compress_method:
        syncoid_cmd.append(f'--compress={compress_method}')

    # Resume (syncoid defaults to resume, add --no-resume if needed)
    if not run_config.get('resume_support', True): # Default to True for syncoid
        syncoid_cmd.append('--no-resume')

    # Bandwidth Limit
    bwlimit = run_config.get('bwlimit')
    if bwlimit:
        try:
            # Ensure bwlimit is an integer (or float convertible to int)
            bwlimit_int = int(float(bwlimit))
            if bwlimit_int > 0:
                 syncoid_cmd.append(f'--mbuffer-size={bwlimit_int}M') # Use mbuffer for rate limiting
                 log_func(f"Applying bandwidth limit: {bwlimit_int} MiB/s via mbuffer", level=logging.DEBUG)
            else:
                 log_func(f"Ignoring invalid bandwidth limit: {bwlimit}", level=logging.WARNING)
        except (ValueError, TypeError):
            log_func(f"Ignoring invalid bandwidth limit format: {bwlimit}", level=logging.WARNING)


    # SSH Options (passed via --sshoption)
    ssh_options = ["-o", f"ConnectTimeout={config['SSH_TIMEOUT']}", "-o", "BatchMode=yes"]
    ssh_extra_options_str = run_config.get('SSH_EXTRA_OPTIONS', '')
    if ssh_extra_options_str:
        try:
            extra_opts_list = shlex.split(ssh_extra_options_str)
            ssh_options.extend(extra_opts_list)
            log_func(f"Using extra SSH options: {extra_opts_list}", level=logging.DEBUG)
        except Exception as e:
            logging.error(f"Could not parse SSH_EXTRA_OPTIONS '{ssh_extra_options_str}': {e} - Ignoring.")
            log_func(f"Warning: Could not parse SSH_EXTRA_OPTIONS: {e}", level=logging.WARNING)

    for opt in ssh_options:
        syncoid_cmd.extend(['--sshoption', opt])

    # Add source and destination arguments
    syncoid_cmd.append(source_arg)
    syncoid_cmd.append(dest_arg)

    # --- Estimate Size (using syncoid -n) ---
    # Size estimation is now handled within execute_syncoid_transfer during dry run
    # total_size is no longer estimated here, execute_syncoid_transfer handles dry run estimate

    # --- Execute Syncoid ---
    log_func(f"Starting syncoid transfer at {datetime.now()}")
    if is_tui_run:
        # Initial TUI state (action might change based on dry run estimate)
        app.update_progress(completed=0, total=None, action="Initializing...")

    try:
        # Execute the syncoid command
        success = execute_syncoid_transfer(syncoid_cmd, run_config, app)

        if success:
            log_func("Syncoid transfer completed successfully.")
            if is_tui_run:
                # Final update handled by stderr thread or execute function
                app.update_progress(completed=app.completed_bytes, total=app.total_bytes, action="Complete")
            return True
        else:
            log_func("Syncoid transfer failed.", level=logging.ERROR)
            if is_tui_run:
                app.update_progress(completed=app.completed_bytes, total=app.total_bytes, action="Failed")
            return False
    except Exception as e:
        log_func(f"Error during syncoid transfer execution: {e}", level=logging.ERROR)
        logging.exception(f"Error during syncoid transfer execution: {e}") # Log traceback
        if is_tui_run:
            app.update_progress(completed=app.completed_bytes, total=app.total_bytes, action="Error")
        return False


# --- Old Transfer Functions (Removed) ---
# Original perform_full_transfer, perform_incremental_transfer, and execute_transfer_pipeline
# functions were here. They have been removed as the logic is now unified in
# perform_transfer using syncoid.