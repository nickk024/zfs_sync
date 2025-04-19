import logging
from dotenv import dotenv_values
from pathlib import Path

def load_configuration(env_file: Path) -> dict:
    """Loads configuration from .env file and parses jobs."""
    config = {}
    job_configs = {}

    if not env_file.is_file():
        logging.warning(f"No .env file found at {env_file}. Using defaults where possible.")
        env_vars = {}
    else:
        logging.info(f"Loading configuration from {env_file}")
        env_vars = dotenv_values(env_file)
        if not env_vars:
             logging.warning(f"Errors encountered while loading {env_file} or file is empty.")

    # --- Load Global Defaults ---
    config['DEFAULT_SSH_USER'] = env_vars.get('DEFAULT_SSH_USER', 'root')
    config['DEFAULT_SNAPSHOT_PREFIX'] = env_vars.get('DEFAULT_SNAPSHOT_PREFIX', 'backup')
    config['DEFAULT_MAX_SNAPSHOTS'] = int(env_vars.get('DEFAULT_MAX_SNAPSHOTS', 5))
    config['DEFAULT_RECURSIVE'] = env_vars.get('DEFAULT_RECURSIVE', 'true').lower() == 'true'
    config['DEFAULT_USE_COMPRESSION'] = env_vars.get('DEFAULT_USE_COMPRESSION', 'true').lower() == 'true'
    config['DEFAULT_RESUME_SUPPORT'] = env_vars.get('DEFAULT_RESUME_SUPPORT', 'true').lower() == 'true'
    # config['DEFAULT_DIRECT_REMOTE_TRANSFER'] = env_vars.get('DEFAULT_DIRECT_REMOTE_TRANSFER', 'false').lower() == 'true' # Removed R2R

    # Global settings
    config['DEBUG_MODE'] = env_vars.get('DEBUG_MODE', 'false').lower() == 'true'
    config['SSH_TIMEOUT'] = int(env_vars.get('SSH_TIMEOUT', 10))
    config['CMD_TIMEOUT'] = int(env_vars.get('CMD_TIMEOUT', 3600)) # Not used yet, but good to have
    config['SSH_EXTRA_OPTIONS'] = env_vars.get('SSH_EXTRA_OPTIONS', '') # Load extra SSH options as a string

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
        job_data['snapshot_prefix'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_SNAPSHOT_PREFIX', config['DEFAULT_SNAPSHOT_PREFIX'])
        job_data['max_snapshots'] = int(env_vars.get(f'ZFS_SYNC_JOB_{job_name}_MAX_SNAPSHOTS', config['DEFAULT_MAX_SNAPSHOTS']))
        job_data['recursive'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_RECURSIVE', str(config['DEFAULT_RECURSIVE'])).lower() == 'true'
        job_data['use_compression'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_USE_COMPRESSION', str(config['DEFAULT_USE_COMPRESSION'])).lower() == 'true'
        job_data['resume_support'] = env_vars.get(f'ZFS_SYNC_JOB_{job_name}_RESUME_SUPPORT', str(config['DEFAULT_RESUME_SUPPORT'])).lower() == 'true'
        job_data['direct_remote_transfer'] = False # Removed R2R
        job_data['sync_snapshot'] = f"{job_data['snapshot_prefix']}-sync" # Derive sync snapshot name

        # Validate Mandatory Job Settings
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

    return config