import logging
import os # Import os for path operations
from dotenv import dotenv_values
from pathlib import Path
from typing import Dict, Any # Import Dict, Any

# --- Configuration Loading ---

def find_config_file() -> Path | None:
    """Searches for the .env configuration file in the current directory."""
    # Only check current directory for .env
    env_path = Path.cwd() / '.env'
    if env_path.is_file():
         return env_path # Return Path object

    logging.error("Configuration file (.env) not found in the current directory.")
    return None

# Configuration is loaded from .env file using python-dotenv
def load_config(config_file_path: Path | str) -> Dict[str, Any]:
    """Loads configuration from .env file and parses jobs."""
    config: Dict[str, Any] = {}
    job_configs: Dict[str, Dict[str, Any]] = {}
    env_file = Path(config_file_path) # Ensure it's a Path object

    if not env_file.is_file():
        logging.error(f"Configuration file not found at {env_file}.")
        # Return empty dict or raise error? Returning empty for now.
        return {}
    else:
        logging.info(f"Loading configuration from {env_file}")
        # Assuming .env format for now
        env_vars = dotenv_values(env_file)
        if not env_vars:
             logging.warning(f"Errors encountered while loading {env_file} or file is empty.")

    # --- Load Global Defaults ---
    config['DEFAULT_SSH_USER'] = env_vars.get('DEFAULT_SSH_USER', 'root')
    config['DEFAULT_SNAPSHOT_PREFIX'] = env_vars.get('DEFAULT_SNAPSHOT_PREFIX', 'backup')
    # config['DEFAULT_MAX_SNAPSHOTS'] = int(env_vars.get('DEFAULT_MAX_SNAPSHOTS', 5)) # Removed - Handled by sanoid.conf
    config['DEFAULT_RECURSIVE'] = env_vars.get('DEFAULT_RECURSIVE', 'true').lower() == 'true'
    config['DEFAULT_USE_COMPRESSION'] = env_vars.get('DEFAULT_USE_COMPRESSION', 'true').lower() == 'true'
    config['DEFAULT_RESUME_SUPPORT'] = env_vars.get('DEFAULT_RESUME_SUPPORT', 'true').lower() == 'true' # Note: Syncoid resumes by default

    # Global settings
    config['LOG_LEVEL'] = env_vars.get('LOG_LEVEL', 'INFO').upper()
    config['LOG_FILE'] = env_vars.get('LOG_FILE') # Can be None
    config['DRY_RUN'] = env_vars.get('DRY_RUN', 'false').lower() == 'true' # Global dry run flag
    config['SSH_TIMEOUT'] = int(env_vars.get('SSH_TIMEOUT', 10))
    # config['CMD_TIMEOUT'] = int(env_vars.get('CMD_TIMEOUT', 3600)) # Keep for potential future use
    config['SSH_EXTRA_OPTIONS'] = env_vars.get('SSH_EXTRA_OPTIONS', '')

    # Paths for sanoid/syncoid (use submodule paths as default)
    config['SANOID_PATH'] = env_vars.get('SANOID_PATH', 'libs/sanoid/sanoid')
    config['SYNCOID_PATH'] = env_vars.get('SYNCOID_PATH', 'libs/sanoid/syncoid')
    config['SANOID_CONF_PATH'] = env_vars.get('SANOID_CONF_PATH', '/etc/sanoid/sanoid.conf') # Standard system path default

    # --- Parse Job Definitions ---
    job_names_str = env_vars.get('ZFS_SYNC_JOB_NAMES', '')
    if not job_names_str:
        logging.warning("ZFS_SYNC_JOB_NAMES is not defined in .env. No jobs loaded.")
        config['JOBS'] = {}
        return config # Return early if no jobs defined

    job_names = job_names_str.split()
    logging.info(f"Found job names: {job_names}")

    for job_name in job_names:
        logging.debug(f"Loading configuration for job: {job_name}")
        job_data = {}
        is_valid = True

        # Read job-specific variables, falling back to globals
        job_data['source_host'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_SOURCE_HOST')
        job_data['source_dataset'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_SOURCE_DATASET')
        job_data['dest_host'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_DEST_HOST')
        job_data['dest_dataset'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_DEST_DATASET')
        job_data['ssh_user'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_SSH_USER', config['DEFAULT_SSH_USER'])
        job_data['snapshot_prefix'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_SNAPSHOT_PREFIX', config['DEFAULT_SNAPSHOT_PREFIX']) # Still used for pre-creation step
        # job_data['max_snapshots'] = int(env_vars.get(f'ZFS_SYNC_JOB_{job_name}_MAX_SNAPSHOTS', config['DEFAULT_MAX_SNAPSHOTS'])) # Removed
        job_data['recursive'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_RECURSIVE', str(config['DEFAULT_RECURSIVE'])).lower() == 'true'
        job_data['use_compression'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_USE_COMPRESSION', str(config['DEFAULT_USE_COMPRESSION'])).lower() == 'true'
        job_data['compression_method'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_COMPRESSION_METHOD', 'lz4') # Add specific method for syncoid
        job_data['resume_support'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_RESUME_SUPPORT', str(config['DEFAULT_RESUME_SUPPORT'])).lower() == 'true' # Controls --no-resume flag
        job_data['bwlimit'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_BWLIMIT') # Bandwidth limit (e.g., 100M) - handled by syncoid
        # job_data['sync_snapshot'] = f"{job_data['snapshot_prefix']}-sync" # Removed

        # Validate Mandatory Job Settings (keep existing checks)
        if not job_data['source_host']: logging.error(f"Job '{job_name}': Missing SOURCE_HOST"); is_valid = False
        if not job_data['source_dataset']: logging.error(f"Job '{job_name}': Missing SOURCE_DATASET"); is_valid = False
        if not job_data['dest_host']: logging.error(f"Job '{job_name}': Missing DEST_HOST"); is_valid = False
        if not job_data['dest_dataset']: logging.error(f"Job '{job_name}': Missing DEST_DATASET"); is_valid = False

        if is_valid:
            job_configs[job_name] = job_data
            logging.debug(f"Successfully loaded configuration for job: {job_name}")
        else:
            logging.warning(f"Skipping job '{job_name}' due to missing mandatory configuration.")

    config['JOBS'] = job_configs
    if not config['JOBS']:
         logging.warning("No valid jobs were configured.")

    config['JOBS'] = job_configs
    if not config['JOBS']:
         logging.warning("No valid jobs were configured.")

    return config


# --- Configuration Validation ---

def validate_job_config(job_config: Dict[str, Any]) -> bool:
    """Validates a single job configuration dictionary."""
    job_name = job_config.get('name', 'Unnamed Job') # Get name if added temporarily
    is_valid = True
    required_keys = ['source_host', 'source_dataset', 'dest_host', 'dest_dataset', 'ssh_user', 'snapshot_prefix', 'recursive'] # Removed max_snapshots
    for key in required_keys:
        if key not in job_config or job_config[key] is None:
            logging.error(f"Job '{job_name}': Missing or empty mandatory configuration key: {key}")
            is_valid = False

    # Add specific type checks if needed (e.g., for recursive boolean)
    if 'recursive' in job_config and not isinstance(job_config['recursive'], bool):
         logging.error(f"Job '{job_name}': 'recursive' setting must be a boolean (true/false). Found: {job_config['recursive']}")
         is_valid = False
    # Add check for bwlimit format if present? Syncoid handles errors, maybe just warn.
    if 'bwlimit' in job_config and job_config['bwlimit']:
        try:
            # Basic check if it looks like a number potentially followed by K/M/G
            limit_str = str(job_config['bwlimit']).upper()
            if not re.match(r'^\d+(\.\d+)?[KMG]?$', limit_str):
                 logging.warning(f"Job '{job_name}': Bandwidth limit '{job_config['bwlimit']}' format might be invalid. Syncoid will validate.")
        except:
             logging.warning(f"Job '{job_name}': Could not parse bandwidth limit '{job_config['bwlimit']}'.")


    # Add checks for path executability? Might be complex for remote paths.
    # Rely on execute_command failures for now.

    return is_valid