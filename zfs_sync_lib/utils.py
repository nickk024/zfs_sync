import logging
import os
import sys
import subprocess
from pathlib import Path

# --- Logging Setup ---
def setup_logging(log_dir: Path, debug_mode: bool = False):
    """Sets up logging configuration."""
    log_dir.mkdir(parents=True, exist_ok=True)
    # Use a more descriptive log file name if possible
    try:
        hostname = os.uname().nodename
    except AttributeError:
        import socket
        hostname = socket.gethostname()
    log_file = log_dir / f"zfs_sync_{hostname}_py.log"
    log_level = logging.DEBUG if debug_mode else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'

    # Configure root logger
    # Remove existing handlers to avoid duplicate logs if called multiple times
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=log_level,
                        format=log_format,
                        handlers=[
                            logging.FileHandler(log_file),
                            logging.StreamHandler(sys.stdout) # Also log to console
                        ])
    logging.info(f"Logging initialized. Log file: {log_file}")

# --- Command Execution ---
def execute_command(command_args: list, host: str = "local", ssh_user: str = None,
                    config: dict = None, check: bool = True, capture_output: bool = True,
                    timeout: int = None, shell: bool = False, input_data: str = None) -> subprocess.CompletedProcess:
    """
    Executes a command locally or remotely via SSH, handling dry-run.

    Args:
        command_args: A list of command arguments (e.g., ['zfs', 'list', '-t', 'snapshot']).
                      If shell=True, this should be a single string.
        host: The host to run the command on ('local' or remote hostname/IP).
        ssh_user: The SSH user for remote commands.
        config: The global configuration dictionary containing DRY_RUN, SSH_TIMEOUT.
        check: If True, raise CalledProcessError on non-zero exit code.
        capture_output: If True, capture stdout and stderr.
        timeout: Command timeout in seconds.
        shell: If True, execute command through the shell (use with caution).
        input_data: String to pass as standard input to the command.

    Returns:
        A subprocess.CompletedProcess object.
    Raises:
        subprocess.CalledProcessError: If check=True and the command returns non-zero.
        subprocess.TimeoutExpired: If the command times out.
        FileNotFoundError: If the command is not found (and shell=False).
        Exception: For other potential errors.
    """
    if config is None: config = {}
    dry_run = config.get('DRY_RUN', False)
    ssh_timeout = config.get('SSH_TIMEOUT', 10)
    # Use job-specific timeout if available, else global, else None
    final_timeout = timeout if timeout is not None else config.get('CMD_TIMEOUT')

    full_command_list_or_str = None
    is_remote = host != "local"
    shell_needed = shell # Keep track if shell is needed

    if is_remote:
        if not ssh_user:
            ssh_user = config.get('DEFAULT_SSH_USER', 'root') # Use default if needed

        # Base SSH command
        ssh_base = [
            "ssh",
            "-o", f"ConnectTimeout={ssh_timeout}",
            "-o", "BatchMode=yes", # Prevent password prompts
            "-o", "StrictHostKeyChecking=no", # Consider security implications
            "-o", "UserKnownHostsFile=/dev/null", # Consider security implications
            f"{ssh_user}@{host}"
        ]

        if shell:
            # If the original command needs a shell, pass it as a single string to ssh
            if not isinstance(command_args, str):
                 # This case is tricky - joining might not preserve shell metachars
                 # Best practice is to avoid shell=True for remote commands if possible
                 logging.warning("Using shell=True with remote command list - joining might break complex commands.")
                 remote_cmd_str = " ".join(command_args)
            else:
                 remote_cmd_str = command_args
            full_command_list_or_str = ssh_base + [remote_cmd_str]
            shell_needed = False # SSH handles the remote shell execution
        else:
            # Pass command args directly to ssh
             full_command_list_or_str = ssh_base + command_args

    else: # Local execution
        full_command_list_or_str = command_args

    # Get a string representation for logging
    if isinstance(full_command_list_or_str, list):
        command_str_log = " ".join(full_command_list_or_str)
    else:
        command_str_log = full_command_list_or_str

    if dry_run:
        logging.info(f"[DRY RUN] Would execute on '{host}': {command_str_log}")
        return subprocess.CompletedProcess(args=full_command_list_or_str, returncode=0, stdout="", stderr="")
    else:
        logging.info(f"Executing on '{host}': {command_str_log}")
        try:
            process = subprocess.run(
                full_command_list_or_str,
                check=check,
                capture_output=capture_output,
                text=True,
                timeout=final_timeout,
                shell=shell_needed, # Use shell only if explicitly needed locally
                input=input_data
            )
            # Log output only in debug mode to avoid clutter
            if process.stdout:
                logging.debug(f"Stdout: {process.stdout.strip()}")
            if process.stderr:
                logging.debug(f"Stderr: {process.stderr.strip()}")
            return process
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed on '{host}' with exit code {e.returncode}: {command_str_log}")
            # Log captured output on error
            if e.stdout: logging.error(f"Stdout: {e.stdout.strip()}")
            if e.stderr: logging.error(f"Stderr: {e.stderr.strip()}")
            if check: raise
            return e
        except subprocess.TimeoutExpired as e:
            logging.error(f"Command timed out on '{host}': {command_str_log}")
            if check: raise
            return e
        except FileNotFoundError as e:
             logging.error(f"Command not found on '{host}': {command_str_log}")
             if check: raise
             return subprocess.CompletedProcess(args=full_command_list_or_str, returncode=127, stdout="", stderr=str(e))
        except Exception as e:
            logging.error(f"An unexpected error occurred executing command on '{host}': {command_str_log}")
            logging.exception("Error details:") # Log full traceback
            if check: raise
            return subprocess.CompletedProcess(args=full_command_list_or_str, returncode=1, stdout="", stderr=str(e))


# --- System Checks ---

def check_command_exists(command: str) -> bool:
    """Checks if a command exists in the system's PATH."""
    try:
        # Use subprocess instead of 'which' for better portability
        subprocess.run([command, '--version'], check=True, capture_output=True, text=True)
        logging.debug(f"Command '{command}' found.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Try 'which' as a fallback for commands that don't support --version
         try:
             subprocess.run(['which', command], check=True, capture_output=True)
             logging.debug(f"Command '{command}' found (via which).")
             return True
         except (subprocess.CalledProcessError, FileNotFoundError):
             logging.debug(f"Command '{command}' not found in PATH.")
             return False

def check_prerequisites(config: dict):
    """Checks for required command-line tools."""
    logging.info("Checking for required tools...")
    required = ["zfs", "ssh", "pv"]
    all_found = True
    for cmd in required:
        if not check_command_exists(cmd):
            logging.error(f"Required command '{cmd}' not found. Please install it.")
            all_found = False

    # Check compression tools only if needed by any job or default
    needs_compression_check = config.get('DEFAULT_USE_COMPRESSION', False)
    if not needs_compression_check and 'JOBS' in config:
        for job_cfg in config['JOBS'].values():
            if job_cfg.get('use_compression', False):
                needs_compression_check = True
                break

    if needs_compression_check:
        has_pigz = check_command_exists("pigz")
        has_gzip = check_command_exists("gzip")
        if has_pigz:
            logging.info("Using pigz for compression (faster parallel gzip).")
        elif has_gzip:
             logging.info("pigz not found. Using gzip for compression.")
        else:
            logging.error("Compression enabled in config, but neither 'pigz' nor 'gzip' found.")
            all_found = False

    # Note: Resume support check (mbuffer) might be needed if not using ZFS native resume

    if not all_found:
        logging.error("One or more required commands not found. Please install them and ensure they are in the PATH.")
        sys.exit(1)

    logging.info("Prerequisite checks completed.")


def verify_ssh(host: str, ssh_user: str, config: dict):
    """Verifies SSH connection and remote ZFS availability."""
    if host == "local":
        return True # No need to verify local

    logging.info(f"Verifying SSH connection and ZFS on {ssh_user}@{host}...")
    try:
        # Check SSH connection and if 'zfs' command exists remotely
        # Use a simple zfs command like 'zfs version' or 'zfs list -H -o name -d 1'
        execute_command(['zfs', 'version'], host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True)
        logging.info(f"SSH connection and remote ZFS verified on {host}.")
        return True
    except Exception as e:
        # execute_command logs details, add specific error here
        logging.error(f"Failed to verify SSH or ZFS on {ssh_user}@{host}. Check connectivity, SSH keys, and ZFS installation.")
        # Optionally try a simpler ssh connection test to differentiate
        try:
             execute_command(['exit', '0'], host=host, ssh_user=ssh_user, config=config, check=True, timeout=5)
             logging.error(f"SSH connection to {ssh_user}@{host} seems OK, but 'zfs' command failed.")
        except Exception:
             logging.error(f"SSH connection to {ssh_user}@{host} failed.")
        sys.exit(1) # Exit on verification failure