import logging
import subprocess
import threading
import select
import re
import shlex
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

# Rich for progress bar
from rich.progress import Progress, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn

# Import necessary functions from other modules
from .utils import execute_command, check_command_exists
from .zfs import estimate_transfer_size, get_receive_resume_token # Import the new functions

# --- Stderr Processing Thread ---

def _parse_zfs_size(size_str: str) -> int:
    """Parses ZFS size string (e.g., 1.23G, 45M, 100K) into bytes."""
    size_str = size_str.upper()
    if size_str.endswith('B'): size_str = size_str[:-1] # Remove trailing B if present

    unit_multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4, 'P': 1024**5}
    unit = 'B' # Default unit bytes

    for u, m in unit_multipliers.items():
        if size_str.endswith(u):
            unit = u
            size_str = size_str[:-1]
            break

    try:
        size = float(size_str)
        if unit != 'B':
            size *= unit_multipliers[unit]
        return int(size)
    except ValueError:
        logging.warning(f"Could not parse ZFS size string: {size_str}")
        return 0

def _process_zfs_stderr(stderr_pipe, progress: Progress, task_id, total_size: Optional[int]):
    """
    Reads zfs send stderr, parses progress, and updates the rich progress bar.
    Runs in a separate thread.
    """
    # Regex to find size information in zfs send -v output
    # Example: "full send of pool/fs@snap size 1.23G completed" -> We need the size part
    # Example: "incremental send of pool/fs@snap size 45.6M completed"
    # A simpler approach might be to look for lines ending in size units
    # Let's try finding lines with size units just before the end or 'completed'
    # Regex: (\d+(\.\d+)?)\s*([KMGTP]?B)[\s]*(?:completed|$)
    # This is still fragile. ZFS send verbose output isn't standardized for parsing.
    # A common pattern is `size\s+(\d+(\.\d+)?[KMGTP]?)`, often near the end.
    size_pattern = re.compile(r"size\s+(\d+(\.\d+)?[KMGTP]?)", re.IGNORECASE)
    total_bytes_reported = 0

    # Use select for non-blocking reads
    poller = select.poll()
    poller.register(stderr_pipe, select.POLLIN)

    while True:
        # Wait for data, but with a timeout to allow checking thread status
        if poller.poll(100): # 100ms timeout
            line = stderr_pipe.readline()
            if not line: # EOF
                break
            line = line.strip()
            logging.debug(f"ZFS Send Stderr: {line}")

            match = size_pattern.search(line)
            if match:
                size_str = match.group(1)
                current_bytes = _parse_zfs_size(size_str)
                if current_bytes > total_bytes_reported:
                    advance = current_bytes - total_bytes_reported
                    logging.debug(f"Parsed size: {size_str} ({current_bytes} bytes), advancing progress by {advance}")
                    progress.update(task_id, advance=advance)
                    total_bytes_reported = current_bytes
                else:
                     logging.debug(f"Parsed size: {size_str} ({current_bytes} bytes), but not advancing (<= previous total {total_bytes_reported})")

        else:
            # Check if pipe is closed from the other end
            if stderr_pipe.closed:
                 break
            # Add a small sleep if no data to prevent busy-waiting entirely
            # time.sleep(0.01)
            pass # Poller timeout handles waiting

    logging.debug("Stderr processing thread finished.")
    # Ensure progress reaches 100% if total size was known and we finished
    if total_size is not None and total_bytes_reported < total_size:
         # Only advance if we are reasonably close, otherwise estimate was bad
         if total_size - total_bytes_reported < total_size * 0.1: # within 10%
              progress.update(task_id, advance=(total_size - total_bytes_reported))
         else:
              logging.warning("Final reported bytes significantly less than estimated total.")
    elif total_size is None:
         # If total size was unknown, just mark as complete
         progress.update(task_id, completed=total_bytes_reported, total=total_bytes_reported) # Mark as finished


def execute_transfer_pipeline(pipeline_cmds: List[List[str]], config: dict, progress: Progress, task_id) -> bool:
    """
    Executes a ZFS transfer pipeline using Popen, captures zfs send stderr for progress.

    Args:
        pipeline_cmds: A list of lists, where each inner list is the command args for a stage.
                       Example: [['zfs', 'send', ...], ['ssh', ...], ['zfs', 'recv', ...]]
        config: The combined job/global config.
        progress: The rich Progress object.
        task_id: The task ID for the rich Progress object.

    Returns:
        True on success, False on failure.
    """
    processes = []
    stderr_pipe_read_end = None
    stderr_thread = None
    success = False
    dry_run = config.get('dry_run', config.get('DRY_RUN', False)) # Check dry run flag

    # The dry run check for action commands is now handled within execute_command.
    # However, the actual pipeline execution using Popen should only happen if not dry run.
    if dry_run:
        # Log the planned pipeline stages (which were determined using real read commands)
        logging.info("[DRY RUN] Planned execution pipeline:")
        for i, cmd_args in enumerate(pipeline_cmds):
            logging.info(f"[DRY RUN] Stage {i}: {' '.join(cmd_args)}")
        # Simulate success for dry run pipeline planning
        progress.update(task_id, completed=progress.tasks[task_id].total or 1, total=progress.tasks[task_id].total or 1)
        return True

    # --- Actual Pipeline Execution (Not Dry Run) ---
    try:
        # Start all processes in the pipeline
        last_stdout = None
        for i, cmd_args in enumerate(pipeline_cmds):
            is_first = i == 0
            is_last = i == len(pipeline_cmds) - 1
            stdin_pipe = last_stdout if not is_first else None
            stdout_pipe = subprocess.PIPE if not is_last else None # Last command stdout goes nowhere unless captured
            stderr_pipe = subprocess.PIPE if is_first else None # Capture stderr only from the first (zfs send)

            logging.debug(f"Starting pipeline stage {i}: {' '.join(cmd_args)}")
            proc = subprocess.Popen(cmd_args, stdin=stdin_pipe, stdout=stdout_pipe, stderr=stderr_pipe, text=True)
            processes.append(proc)

            if is_first:
                stderr_pipe_read_end = proc.stderr
                # Start the stderr processing thread
                total_size = progress.tasks[task_id].total
                stderr_thread = threading.Thread(target=_process_zfs_stderr, args=(stderr_pipe_read_end, progress, task_id, total_size), daemon=True)
                stderr_thread.start()

            # Close the previous process's stdout pipe after it's passed to the current one
            if last_stdout:
                last_stdout.close()

            last_stdout = proc.stdout # Prepare for the next stage

        # Wait for all processes to complete and check exit codes
        final_return_code = 0
        for i, proc in enumerate(processes):
            proc.wait() # Wait for process to finish
            if proc.returncode != 0:
                logging.error(f"Pipeline stage {i} ({' '.join(pipeline_cmds[i])}) failed with exit code {proc.returncode}")
                final_return_code = proc.returncode # Capture first non-zero exit code
                # Optionally capture/log stderr from failed process if not already done
                if i > 0 and proc.stderr: # We only capture stderr for first process by default
                     try:
                          stderr_output = proc.stderr.read()
                          logging.error(f"Stderr (Stage {i}): {stderr_output.strip()}")
                     except: pass # Ignore errors reading stderr

        # Wait for the stderr processing thread to finish
        if stderr_thread:
            if stderr_pipe_read_end: # Ensure pipe is closed so thread can exit EOF
                 stderr_pipe_read_end.close()
            stderr_thread.join(timeout=5) # Wait max 5 seconds for thread
            if stderr_thread.is_alive():
                 logging.warning("Stderr processing thread did not finish cleanly.")

        success = final_return_code == 0

    except Exception as e:
        logging.exception(f"Error executing transfer pipeline: {e}")
        success = False
    finally:
        # Ensure all processes are terminated if something went wrong
        for proc in processes:
            if proc.poll() is None: # If process is still running
                logging.warning(f"Terminating pipeline process PID {proc.pid}")
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    logging.warning(f"Killing pipeline process PID {proc.pid}")
                    proc.kill()
        # Ensure stderr pipe is closed
        if stderr_pipe_read_end and not stderr_pipe_read_end.closed:
             stderr_pipe_read_end.close()
        # Ensure thread is joined
        if stderr_thread and stderr_thread.is_alive():
             stderr_thread.join(timeout=1)

    return success


# --- Transfer Functions ---

# (get_compression_commands remains the same)
def get_compression_commands(config: dict) -> tuple:
    """Gets the appropriate compression/decompression commands."""
    compress_cmd_list, decompress_cmd_list = None, None
    # Use job-specific or default compression setting
    use_compression = config.get('use_compression', config.get('DEFAULT_USE_COMPRESSION', False))
    if use_compression:
        if check_command_exists("pigz"):
            compress_cmd_list = ["pigz"]
            decompress_cmd_list = ["pigz", "-d"]
        elif check_command_exists("gzip"):
             compress_cmd_list = ["gzip"]
             decompress_cmd_list = ["gzip", "-d"]
    return compress_cmd_list, decompress_cmd_list


def perform_full_transfer(job_config: dict, config: dict, new_snapshot_name: str) -> bool:
    """Performs a full ZFS transfer (initial replication) with Rich progress."""
    src_host = job_config['source_host']
    src_dataset = job_config['source_dataset']
    dst_host = job_config['dest_host']
    dst_dataset = job_config['dest_dataset']
    ssh_user = job_config['ssh_user']
    recursive = job_config['recursive']
    run_config = {**config, **job_config}

    logging.info("Performing initial full transfer...")

    # --- Estimate Size ---
    total_size = estimate_transfer_size(src_dataset, src_host, ssh_user, run_config, new_snapshot=new_snapshot_name)
    if total_size is None:
        logging.warning("Could not estimate transfer size. Progress bar may be indeterminate.")

    # --- Prepare Commands ---
    send_cmd_base_list = ['zfs', 'send', '-p'] # Add -p to preserve properties
    recv_cmd_base_list = ['zfs', 'receive', '-s', '-u', '-v']

    # Check for resume token if resume support is enabled
    resume_token = None
    send_args_list = []
    using_resume_token = False
    if job_config.get('resume_support', False):
        logging.info("Resume support enabled, checking for token on destination...")
        resume_token = get_receive_resume_token(dst_dataset, dst_host, ssh_user, run_config)
        if resume_token:
            logging.info(f"Found resume token. Attempting to resume transfer with 'zfs send -t {resume_token}'")
            send_args_list = ['-t', resume_token]
            using_resume_token = True
            # Size estimation might be inaccurate or impossible for resumed transfers
            if total_size is not None:
                logging.warning("Resuming transfer, initial size estimate may not reflect remaining data.")
                # Optionally reset total_size to None for indeterminate progress bar?
                # total_size = None
        else:
            logging.info("No resume token found on destination. Proceeding with normal full send.")
            send_args_list = [f"{src_dataset}@{new_snapshot_name}"]
    else:
        # Resume not enabled, use standard full send
        send_args_list = [f"{src_dataset}@{new_snapshot_name}"]

    send_cmd_list = send_cmd_base_list[:]
    # Add flags like -R only if NOT using a resume token (-t implies the original flags)
    # Add -v for progress parsing only if NOT using a resume token (stderr format differs)
    if not using_resume_token:
        if recursive: send_cmd_list.append('-R')
        send_cmd_list.append('-v') # Add verbose for progress parsing
    send_cmd_list.extend(send_args_list)

    # --- Construct Pipeline Stages ---
    pipeline_stages = []
    compress_cmd_list, decompress_cmd_list = get_compression_commands(run_config)

    # Build source command list
    current_stage_cmd = send_cmd_list[:]
    pipeline_stages.append(current_stage_cmd)

    if compress_cmd_list:
        pipeline_stages.append(compress_cmd_list)

    # Intermediate/Destination stages
    # Base SSH options - Secure by default
    ssh_base_opts = ["-o", f"ConnectTimeout={config['SSH_TIMEOUT']}", "-o", "BatchMode=yes"]
    # Add extra options from config if provided
    ssh_extra_options_str = run_config.get('SSH_EXTRA_OPTIONS', '')
    if ssh_extra_options_str:
        try:
            extra_opts_list = shlex.split(ssh_extra_options_str)
            ssh_base_opts.extend(extra_opts_list)
        except Exception as e:
            logging.error(f"Could not parse SSH_EXTRA_OPTIONS '{ssh_extra_options_str}' for pipeline: {e} - Ignoring.")

    if src_host == "local" and dst_host == "local":
        if decompress_cmd_list: pipeline_stages.append(decompress_cmd_list)
        pipeline_stages.append(recv_cmd_base_list + [dst_dataset])
    elif src_host == "local":
        remote_cmd_list = []
        if decompress_cmd_list: remote_cmd_list.extend(decompress_cmd_list + ['|']) # Pipe within remote shell
        remote_cmd_list.extend(recv_cmd_base_list + [dst_dataset])
        # Join the remote command list into a single string for SSH execution
        remote_cmd_str = " ".join(remote_cmd_list)
        pipeline_stages.append(["ssh"] + ssh_base_opts + [f"{ssh_user}@{dst_host}", remote_cmd_str])
    elif dst_host == "local":
        # Need to run the source part via SSH
        pipeline_stages.pop() # Remove local send command
        remote_cmd_str = " ".join(current_stage_cmd) # Send command
        if compress_cmd_list: remote_cmd_str += f" | {' '.join(compress_cmd_list)}"
        pipeline_stages.insert(0, ["ssh"] + ssh_base_opts + [f"{ssh_user}@{src_host}", remote_cmd_str]) # Add SSH send at beginning
        # Add decompression and receive locally
        if decompress_cmd_list: pipeline_stages.append(decompress_cmd_list)
        pipeline_stages.append(recv_cmd_base_list + [dst_dataset])
    else: # Both hosts are remote
        logging.error("Remote-to-remote transfers are not supported in this version.")
        return False


    # --- Execute Pipeline with Progress ---
    logging.info(f"Starting full transfer at {datetime.now()}")
    try:
        with Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            transient=True, # Clear progress bar on exit
        ) as progress:
            task_id = progress.add_task("[red]Transferring...", total=total_size)
            success = execute_transfer_pipeline(pipeline_stages, run_config, progress, task_id)

        if success:
            logging.info("Full transfer completed successfully.")
            return True
        else:
            logging.error("Full transfer pipeline failed.")
            return False
    except Exception as e:
        logging.exception(f"Error during full transfer execution: {e}")
        return False


def perform_incremental_transfer(job_config: dict, config: dict, new_snapshot_name: str, base_snapshot_name: str) -> bool:
    """Performs an incremental ZFS transfer with Rich progress."""
    src_host = job_config['source_host']
    src_dataset = job_config['source_dataset']
    dst_host = job_config['dest_host']
    dst_dataset = job_config['dest_dataset']
    ssh_user = job_config['ssh_user']
    recursive = job_config['recursive']
    run_config = {**config, **job_config}

    logging.info("Performing incremental transfer...")
    logging.info(f"Using snapshots: {src_dataset}@{base_snapshot_name} -> {src_dataset}@{new_snapshot_name}")
    # Removed force_rollback logic

    # --- Estimate Size ---
    total_size = estimate_transfer_size(src_dataset, src_host, ssh_user, run_config,
                                        base_snapshot=base_snapshot_name, new_snapshot=new_snapshot_name)
    if total_size is None:
        logging.warning("Could not estimate transfer size. Progress bar may be indeterminate.")

    # --- Prepare Commands ---
    send_cmd_base_list = ['zfs', 'send', '-p'] # Add -p to preserve properties
    recv_cmd_base_list = ['zfs', 'receive', '-u', '-v'] # -u update, -v verbose
    # Removed force_rollback logic adding -F

    if recursive: send_cmd_base_list.append('-R')
    # Add incremental flags - quoting handled by Popen
    send_cmd_base_list.extend(['-i', f"{src_dataset}@{base_snapshot_name}", f"{src_dataset}@{new_snapshot_name}"])

    # --- Construct Pipeline Stages ---
    pipeline_stages = []
    compress_cmd_list, decompress_cmd_list = get_compression_commands(run_config)

    # Build source command list
    current_stage_cmd = send_cmd_base_list[:]
    pipeline_stages.append(current_stage_cmd)

    if compress_cmd_list:
        pipeline_stages.append(compress_cmd_list)

    # Intermediate/Destination stages (similar logic to full transfer)
    # Base SSH options - Secure by default (same logic as full transfer)
    ssh_base_opts = ["-o", f"ConnectTimeout={config['SSH_TIMEOUT']}", "-o", "BatchMode=yes"]
    ssh_extra_options_str = run_config.get('SSH_EXTRA_OPTIONS', '')
    if ssh_extra_options_str:
        try:
            extra_opts_list = shlex.split(ssh_extra_options_str)
            ssh_base_opts.extend(extra_opts_list)
        except Exception as e:
            logging.error(f"Could not parse SSH_EXTRA_OPTIONS '{ssh_extra_options_str}' for pipeline: {e} - Ignoring.")

    if src_host == "local" and dst_host == "local":
        if decompress_cmd_list: pipeline_stages.append(decompress_cmd_list)
        pipeline_stages.append(recv_cmd_base_list + [dst_dataset])
    elif src_host == "local":
        remote_cmd_list = []
        if decompress_cmd_list: remote_cmd_list.extend(decompress_cmd_list + ['|'])
        remote_cmd_list.extend(recv_cmd_base_list + [dst_dataset])
        remote_cmd_str = " ".join(remote_cmd_list)
        pipeline_stages.append(["ssh"] + ssh_base_opts + [f"{ssh_user}@{dst_host}", remote_cmd_str])
    elif dst_host == "local":
        pipeline_stages.pop() # Remove local send
        remote_cmd_list = send_cmd_base_list[:] # Base send command
        if compress_cmd_list: remote_cmd_list = remote_cmd_list + ['|'] + compress_cmd_list
        remote_cmd_str = " ".join(remote_cmd_list) # Join for SSH
        pipeline_stages.insert(0, ["ssh"] + ssh_base_opts + [f"{ssh_user}@{src_host}", remote_cmd_str])
        if decompress_cmd_list: pipeline_stages.append(decompress_cmd_list)
        pipeline_stages.append(recv_cmd_base_list + [dst_dataset])
    else: # Both hosts are remote
        logging.error("Remote-to-remote transfers are not supported in this version.")
        return False


    # --- Execute Pipeline with Progress ---
    logging.info(f"Starting incremental transfer at {datetime.now()}")
    try:
        with Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
             transient=True,
        ) as progress:
            task_id = progress.add_task("[green]Incrementing...", total=total_size)
            success = execute_transfer_pipeline(pipeline_stages, run_config, progress, task_id)

        if success:
            logging.info("Incremental transfer completed successfully.")
            return True
        else:
            logging.error("Incremental transfer pipeline failed.")
            # Check specific error? execute_transfer_pipeline should log details
            return False
    except Exception as e:
        logging.exception(f"Error during incremental transfer execution: {e}")
        return False