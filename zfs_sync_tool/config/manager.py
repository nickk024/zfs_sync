import toml
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILENAME = "zfs_sync_config.toml"
DEFAULT_CONFIG_DIRS = [
    os.path.join(os.path.expanduser("~"), ".config", "zfs_sync_tool"), # User config dir
    "/etc/zfs_sync_tool", # System-wide config dir (Linux/Unix)
    "." # Current directory (for local testing/override)
]

DEFAULT_CONFIG: Dict[str, Any] = {
    "general": {
        "log_level": "INFO",
        "log_file": None, # Set path like "/var/log/zfs_sync_tool.log" or None to disable file logging
    },
    "notifications": {
        "email_enabled": False,
        "smtp_server": "smtp.example.com",
        "smtp_port": 587,
        "smtp_user": None,
        "smtp_password": None, # Consider secure storage for passwords
        "sender_email": "zfs-sync@example.com",
        "recipient_emails": [], # List of emails to notify
    },
    "hosts": [
        # Example host entry (add actual hosts here)
        # {
        #     "name": "local",
        #     "address": "localhost",
        #     "is_local": True,
        #     "ssh_user": None,
        #     "ssh_key_path": None,
        # },
        # {
        #     "name": "remote-backup",
        #     "address": "backup.server.com",
        #     "is_local": False,
        #     "ssh_user": "backupuser",
        #     "ssh_key_path": "~/.ssh/id_rsa_backup",
        # }
    ],
    "sync_tasks": [
        # Example sync task entry
        # {
        #     "name": "Sync MyData to Backup",
        #     "source_host": "local", # Name matching a host in 'hosts' list
        #     "source_dataset": "pool/mydata",
        #     "target_host": "remote-backup",
        #     "target_dataset": "backup_pool/mydata_backup",
        #     "enabled": True,
        #     "schedule": "0 2 * * *", # Cron-like schedule (e.g., every day at 2 AM)
        #     "options": {
        #         "recursive": True,
        #         "compression": "lz4",
        #         "bandwidth_limit": "50m", # 50 MB/s
        #         "no_sync_snap": False,
        #         "create_bookmark": False,
        #         "delete_target_snapshots": True,
        #     }
        # }
    ]
}

def find_config_file(filename: str = DEFAULT_CONFIG_FILENAME, search_dirs: Optional[List[str]] = None) -> Optional[str]:
    """Finds the configuration file in predefined locations."""
    if search_dirs is None:
        search_dirs = DEFAULT_CONFIG_DIRS
    for config_dir in search_dirs:
        config_path = os.path.join(config_dir, filename)
        if os.path.isfile(config_path):
            logger.debug(f"Found configuration file at: {config_path}")
            return config_path
    logger.debug(f"Configuration file '{filename}' not found in search directories: {search_dirs}")
    return None

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads the application configuration from a TOML file.

    If config_path is not provided, it searches in default locations.
    If no file is found, it returns the default configuration.

    Args:
        config_path: The specific path to the configuration file.

    Returns:
        A dictionary containing the loaded configuration. Merges loaded
        config with defaults, prioritizing loaded values.
    """
    if config_path is None:
        config_path = find_config_file()

    loaded_config = {}
    if config_path and os.path.isfile(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                loaded_config = toml.load(f)
            logger.info(f"Loaded configuration from: {config_path}")
        except toml.TomlDecodeError as e:
            logger.error(f"Error decoding TOML file {config_path}: {e}")
            # Fallback to default config on decode error
        except IOError as e:
            logger.error(f"Error reading configuration file {config_path}: {e}")
            # Fallback to default config on read error
    else:
        logger.warning(f"Configuration file not found or specified path invalid. Using default configuration.")

    # Merge loaded config with defaults (deep merge might be needed for nested dicts if desired)
    # For simplicity here, we'll just update the top level
    config = DEFAULT_CONFIG.copy()
    config.update(loaded_config) # Loaded values overwrite defaults at the top level

    # TODO: Add validation logic here (e.g., using Pydantic or jsonschema)

    return config

def save_config(config: Dict[str, Any], config_path: str) -> bool:
    """
    Saves the application configuration to a TOML file.

    Args:
        config: The configuration dictionary to save.
        config_path: The path where the configuration file should be saved.

    Returns:
        True if saving was successful, False otherwise.
    """
    try:
        # Ensure the directory exists
        config_dir = os.path.dirname(config_path)
        if config_dir: # Avoid error if saving to current dir (dirname is '')
             os.makedirs(config_dir, exist_ok=True)

        with open(config_path, 'w', encoding='utf-8') as f:
            toml.dump(config, f)
        logger.info(f"Configuration successfully saved to: {config_path}")
        return True
    except IOError as e:
        logger.error(f"Error writing configuration file {config_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while saving config to {config_path}: {e}")
        return False

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    print("--- Loading Configuration ---")
    # Try loading from default locations first
    current_config = load_config()
    print("Loaded config (defaults merged):")
    import json
    print(json.dumps(current_config, indent=4))

    print("\n--- Saving Configuration ---")
    # Example: Modify a setting and save to a specific file
    current_config["general"]["log_level"] = "DEBUG"
    save_path = "./zfs_sync_config_saved.toml"
    if save_config(current_config, save_path):
        print(f"Configuration saved to {save_path}")

        print("\n--- Reloading Saved Configuration ---")
        reloaded_config = load_config(save_path)
        print("Reloaded config:")
        print(json.dumps(reloaded_config, indent=4))

        # Clean up saved file
        # try:
        #     os.remove(save_path)
        #     print(f"\nRemoved saved config file: {save_path}")
        # except OSError as e:
        #     print(f"Error removing saved config file {save_path}: {e}")

    else:
        print(f"Failed to save configuration to {save_path}")