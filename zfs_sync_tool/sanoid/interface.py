import logging
import re
import subprocess
from typing import Dict, Any, Optional, Tuple, List

logger = logging.getLogger(__name__)

# Basic regex to capture sections like [data/home] or [template_production]
SECTION_RE = re.compile(r"^\s*\[\s*(.+?)\s*\]\s*$")
# Basic regex to capture key-value pairs like hourly = 36
KEY_VALUE_RE = re.compile(r"^\s*(\w+)\s*=\s*(.*?)\s*$")
# Regex to capture comments
COMMENT_RE = re.compile(r"^\s*#.*$")

def parse_sanoid_conf(config_path: str = "/etc/sanoid/sanoid.conf") -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Parses a Sanoid configuration file.

    Note: Sanoid's config format is similar to TOML but has nuances.
          This parser is basic and might need refinement for complex cases.

    Args:
        config_path: The path to the sanoid.conf file.

    Returns:
        A dictionary representing the parsed configuration, where keys are
        section names (datasets or templates) and values are dictionaries
        of their settings. Returns None if the file cannot be read or parsed.
    """
    config_data: Dict[str, Dict[str, Any]] = {}
    current_section_name: Optional[str] = None

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                if not line or COMMENT_RE.match(line):
                    continue  # Skip empty lines and comments

                section_match = SECTION_RE.match(line)
                if section_match:
                    current_section_name = section_match.group(1)
                    if current_section_name in config_data:
                        logger.warning(f"Duplicate section '{current_section_name}' found at line {line_num} in {config_path}. Overwriting previous definition.")
                    config_data[current_section_name] = {}
                    logger.debug(f"Parsing section: [{current_section_name}]")
                    continue

                key_value_match = KEY_VALUE_RE.match(line)
                if key_value_match and current_section_name:
                    key = key_value_match.group(1)
                    value_str = key_value_match.group(2)
                    value: Any = value_str # Store as string initially
                    # Attempt basic type conversion (can be expanded)
                    if value_str.isdigit():
                        value = int(value_str)
                    elif value_str.lower() in ['yes', 'true']:
                        value = True
                    elif value_str.lower() in ['no', 'false']:
                        value = False
                    config_data[current_section_name][key] = value
                    logger.debug(f"  Parsed '{key}' = {value} (raw: '{value_str}')")
                    continue

                if current_section_name:
                     logger.warning(f"Could not parse line {line_num} in section '{current_section_name}' of {config_path}: {line}")
                else:
                     logger.warning(f"Line {line_num} outside of any section in {config_path}: {line}")


        logger.info(f"Successfully parsed Sanoid config: {config_path}")
        return config_data

    except FileNotFoundError:
        logger.error(f"Sanoid configuration file not found: {config_path}")
        return None
    except IOError as e:
        logger.error(f"Error reading Sanoid configuration file {config_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while parsing {config_path}: {e}")
        return None

def _run_sanoid_command(args: list[str], config_dir: Optional[str] = None) -> Tuple[bool, str, str]:
    """Helper function to run sanoid commands."""
    command = ["sanoid"]
    if config_dir:
        command.extend(["--configdir", config_dir])
    command.extend(args)

    try:
        logger.info(f"Running Sanoid command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        logger.debug(f"Sanoid command stdout:\n{result.stdout}")
        if result.stderr:
             logger.debug(f"Sanoid command stderr:\n{result.stderr}")
        return True, result.stdout, result.stderr
    except FileNotFoundError:
        logger.error("Sanoid command not found. Is Sanoid installed and in the system's PATH?")
        return False, "", "Sanoid command not found."
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Sanoid command {' '.join(command)}: {e}")
        logger.error(f"Stderr: {e.stderr}")
        return False, e.stdout, e.stderr
    except Exception as e:
        logger.error(f"An unexpected error occurred while running Sanoid command {' '.join(command)}: {e}")
        return False, "", str(e)


def take_snapshots(config_dir: Optional[str] = None, verbose: bool = False, debug: bool = False) -> Tuple[bool, str, str]:
    """
    Runs 'sanoid --take-snapshots' to create snapshots based on the configuration.

    Args:
        config_dir: Path to the directory containing sanoid.conf. Defaults to /etc/sanoid.
        verbose: Enable verbose output from sanoid.
        debug: Enable debug output from sanoid.

    Returns:
        A tuple containing:
        - bool: True if the command executed successfully (exit code 0), False otherwise.
        - str: The standard output of the command.
        - str: The standard error of the command.
    """
    args = ["--take-snapshots"]
    if verbose:
        args.append("--verbose")
    if debug:
        args.append("--debug")

    return _run_sanoid_command(args, config_dir)

def prune_snapshots(config_dir: Optional[str] = None, verbose: bool = False, debug: bool = False, force: bool = False) -> Tuple[bool, str, str]:
    """
    Runs 'sanoid --prune-snapshots' to remove expired snapshots based on the configuration.

    Args:
        config_dir: Path to the directory containing sanoid.conf. Defaults to /etc/sanoid.
        verbose: Enable verbose output from sanoid.
        debug: Enable debug output from sanoid.
        force: Use --force-prune to prune even if send/recv is in progress.

    Returns:
        A tuple containing:
        - bool: True if the command executed successfully (exit code 0), False otherwise.
        - str: The standard output of the command.
        - str: The standard error of the command.
    """
    args = ["--prune-snapshots"]
    if force:
        args.append("--force-prune")
    if verbose:
        args.append("--verbose")
    if debug:
        args.append("--debug")

    return _run_sanoid_command(args, config_dir)

def run_syncoid(
    source: str,
    destination: str,
    ssh_options: Optional[List[str]] = None,
    recursive: bool = False,
    skip_parent: bool = False,
    no_sync_snap: bool = False,
    use_hold: bool = False,
    create_bookmark: bool = False,
    delete_target_snapshots: bool = False,
    bandwidth_limit: Optional[str] = None, # e.g., "10m"
    compression: Optional[str] = None, # e.g., "lz4"
    extra_args: Optional[List[str]] = None, # For less common options
    debug: bool = False,
    quiet: bool = False
) -> Tuple[bool, str, str]:
    """
    Runs the 'syncoid' command to replicate datasets.

    Args:
        source: Source dataset (e.g., 'pool/data' or 'user@host:pool/data').
        destination: Destination dataset (e.g., 'backup/data' or 'user@host:backup/data').
        ssh_options: List of options to pass to ssh (e.g., ['-p', '2222', '-i', '/path/to/key']).
        recursive: Replicate child datasets.
        skip_parent: Skip syncing the parent dataset (requires recursive=True).
        no_sync_snap: Use existing snapshots instead of creating a temporary one.
        use_hold: Add a hold to the newest snapshot after replication.
        create_bookmark: Create a bookmark after successful replication (requires no_sync_snap=True).
        delete_target_snapshots: Delete snapshots on the target that don't exist on the source.
        bandwidth_limit: Bandwidth limit (e.g., '10m', '1g'). Applied to target by default.
        compression: Compression method (e.g., 'lz4', 'zstd-fast', 'none').
        extra_args: List of additional arguments to pass directly to syncoid.
        debug: Enable debug output.
        quiet: Suppress non-error output.

    Returns:
        A tuple containing:
        - bool: True if the command executed successfully (exit code 0), False otherwise.
        - str: The standard output of the command.
        - str: The standard error of the command.
    """
    command = ["syncoid"]

    # Add SSH options if provided
    if ssh_options:
        for option in ssh_options:
            # Handle different ways SSH options might be passed
            if option.startswith('-'): # Assume single flag like -p or -i
                 command.extend(["--sshoption", option])
            else: # Assume it's the value for the previous flag
                 command[-1] = f"{command[-1]} {option}" # Combine flag and value for --sshoption

    # Add boolean flags
    if recursive:
        command.append("--recursive")
    if skip_parent and recursive: # skip-parent only works with recursive
        command.append("--skip-parent")
    if no_sync_snap:
        command.append("--no-sync-snap")
    if use_hold:
        command.append("--use-hold")
    if create_bookmark and no_sync_snap: # create-bookmark only works with no-sync-snap
        command.append("--create-bookmark")
    if delete_target_snapshots:
        command.append("--delete-target-snapshots")
    if debug:
        command.append("--debug")
    if quiet:
        command.append("--quiet")

    # Add options with values
    if bandwidth_limit:
        # Syncoid applies --target-bwlimit by default if only one limit is given
        command.extend(["--target-bwlimit", bandwidth_limit])
    if compression:
        command.extend(["--compress", compression])

    # Add extra arguments
    if extra_args:
        command.extend(extra_args)

    # Add source and destination
    command.extend([source, destination])

    try:
        logger.info(f"Running Syncoid command: {' '.join(command)}")
        # Use check=False initially as syncoid might return non-zero on warnings/info
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8')
        logger.debug(f"Syncoid command stdout:\n{result.stdout}")
        if result.stderr:
             logger.debug(f"Syncoid command stderr:\n{result.stderr}")

        # Determine success based on exit code (0 is success)
        success = result.returncode == 0
        if not success:
             logger.error(f"Syncoid command failed with exit code {result.returncode}")
             logger.error(f"Stderr: {result.stderr}")

        return success, result.stdout, result.stderr
    except FileNotFoundError:
        logger.error("Syncoid command not found. Is Syncoid installed and in the system's PATH?")
        return False, "", "Syncoid command not found."
    except Exception as e:
        logger.error(f"An unexpected error occurred while running Syncoid command {' '.join(command)}: {e}")
        return False, "", str(e)


if __name__ == '__main__':
    # Example usage (for testing purposes)
    logging.basicConfig(level=logging.DEBUG)

    # --- Test parse_sanoid_conf ---
    dummy_conf_path = "sanoid.conf.example"
    dummy_content = """
# This is a comment
[data/web]
    use_template = production
    recursive = yes

[data/db]
    use_template = production
    hourly = 12 # Override template

# Template definitions
[template_production]
    frequently = 0
    hourly = 24
    daily = 7
    monthly = 1
    yearly = 0
    autosnap = yes
    autoprune = yes

[template_backup]
    daily = 14
    monthly = 6
    yearly = 1
    autosnap = no
    autoprune = yes
    """
    try:
        with open(dummy_conf_path, "w", encoding='utf-8') as f:
            f.write(dummy_content)
        print(f"Created dummy config file: {dummy_conf_path}")

        parsed_config = parse_sanoid_conf(dummy_conf_path)

        if parsed_config:
            print("\nParsed Configuration:")
            import json
            print(json.dumps(parsed_config, indent=4))
        else:
            print("\nFailed to parse configuration.")

    except Exception as e:
        print(f"Error during config parsing example execution: {e}")

    # --- Test Sanoid commands (will likely fail if sanoid isn't configured/runnable) ---
    dummy_config_dir = "./dummy_sanoid_config"
    import os
    import shutil
    try:
        print(f"\n--- Testing Sanoid Commands (using dummy config dir: {dummy_config_dir}) ---")
        os.makedirs(dummy_config_dir, exist_ok=True)
        with open(os.path.join(dummy_config_dir, "sanoid.conf"), "w", encoding='utf-8') as f:
             f.write(dummy_content)
        # Need a defaults file too for sanoid to run
        with open(os.path.join(dummy_config_dir, "sanoid.defaults.conf"), "w", encoding='utf-8') as f:
             f.write("[template_default]\n autosnap = no\n autoprune = no")

        print("\nAttempting 'sanoid --take-snapshots':")
        success_take, stdout_take, stderr_take = take_snapshots(config_dir=dummy_config_dir, verbose=True)
        print(f"Execution Success: {success_take}")
        print(f"STDOUT:\n{stdout_take}")
        print(f"STDERR:\n{stderr_take}")

        print("\nAttempting 'sanoid --prune-snapshots':")
        success_prune, stdout_prune, stderr_prune = prune_snapshots(config_dir=dummy_config_dir, verbose=True)
        print(f"Execution Success: {success_prune}")
        print(f"STDOUT:\n{stdout_prune}")
        print(f"STDERR:\n{stderr_prune}")

    except Exception as e:
        print(f"Error during Sanoid command example execution: {e}")
    # No finally block here, keep dummy dir for syncoid test

    # --- Test Syncoid command (will likely fail without actual ZFS pools/datasets) ---
    try:
        print("\n--- Testing Syncoid Command ---")
        # Example: Simulate replicating a local dataset recursively with compression
        # NOTE: This command WILL FAIL unless you have ZFS pools named 'tank/source_data'
        #       and 'tank/backup_data' and syncoid installed.
        print("\nAttempting 'syncoid tank/source_data tank/backup_data --recursive --compress lz4':")
        sync_success, sync_stdout, sync_stderr = run_syncoid(
            source="tank/source_data",
            destination="tank/backup_data",
            recursive=True,
            compression="lz4",
            debug=True # Add debug for more info on failure
        )
        print(f"Execution Success: {sync_success}")
        print(f"STDOUT:\n{sync_stdout}")
        print(f"STDERR:\n{sync_stderr}")

    except Exception as e:
        print(f"Error during Syncoid command example execution: {e}")
    finally:
        # Clean up dummy dir and files from previous tests
        if os.path.exists(dummy_config_dir):
            shutil.rmtree(dummy_config_dir)
            print(f"\nRemoved dummy config dir: {dummy_config_dir}")
        if os.path.exists(dummy_conf_path):
            os.remove(dummy_conf_path)
            print(f"Removed dummy config file: {dummy_conf_path}")


    # --- Test parsing default config location ---
    print(f"\nAttempting to parse default config: /etc/sanoid/sanoid.conf")
    default_config = parse_sanoid_conf()
    if default_config:
        print("Successfully parsed default config (content not shown).")
    else:
        print("Could not parse default config (may not exist or permissions issue).")