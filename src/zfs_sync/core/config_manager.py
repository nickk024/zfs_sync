"""
Configuration Manager Module

This module provides functions for managing configurations for the ZFS sync tool.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger('zfs_sync.core.config_manager')

class ConfigError(Exception):
    """Exception raised for errors in configuration operations."""
    pass

def get_config_dir() -> Path:
    """
    Get the configuration directory.
    
    Returns:
        Path to the configuration directory
    """
    config_dir = Path.home() / '.zfs_sync'
    config_dir.mkdir(exist_ok=True)
    return config_dir

def get_config_path() -> Path:
    """
    Get the path to the configuration file.
    
    Returns:
        Path to the configuration file
    """
    return get_config_dir() / 'config.json'

def load_config() -> Dict[str, Any]:
    """
    Load the configuration from the configuration file.
    
    Returns:
        Configuration dictionary
    """
    config_path = get_config_path()
    
    if not config_path.exists():
        return create_default_config()
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate the configuration
        validate_config(config)
        
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse configuration file: {e}")
        raise ConfigError(f"Failed to parse configuration file: {e}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ConfigError(f"Failed to load configuration: {e}")

def save_config(config: Dict[str, Any]) -> None:
    """
    Save the configuration to the configuration file.
    
    Args:
        config: Configuration dictionary
    """
    config_path = get_config_path()
    
    try:
        # Validate the configuration before saving
        validate_config(config)
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        
        logger.info(f"Configuration saved to {config_path}")
    except Exception as e:
        logger.error(f"Failed to save configuration: {e}")
        raise ConfigError(f"Failed to save configuration: {e}")

def create_default_config() -> Dict[str, Any]:
    """
    Create a default configuration.
    
    Returns:
        Default configuration dictionary
    """
    config = {
        "version": 1,
        "default_source_dataset": "tank/media",
        "default_destination_server": "localhost",
        "default_destination_dataset": "backup/media",
        "sync_options": {
            "recursive": True,
            "compress": "lz4",
            "create-bookmark": True
        },
        "sanoid": {
            "enabled": True,
            "config_path": str(get_config_dir() / 'sanoid.conf')
        },
        "saved_configurations": []
    }
    
    # Save the default configuration
    try:
        config_path = get_config_path()
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        
        logger.info(f"Default configuration created at {config_path}")
    except Exception as e:
        logger.error(f"Failed to create default configuration: {e}")
        raise ConfigError(f"Failed to create default configuration: {e}")
    
    return config

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate the configuration.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigError: If the configuration is invalid
    """
    required_keys = [
        "version",
        "default_source_dataset",
        "default_destination_server",
        "default_destination_dataset",
        "sync_options",
        "sanoid",
        "saved_configurations"
    ]
    
    for key in required_keys:
        if key not in config:
            raise ConfigError(f"Missing required configuration key: {key}")
    
    # Validate sync_options
    if not isinstance(config["sync_options"], dict):
        raise ConfigError("sync_options must be a dictionary")
    
    # Validate sanoid
    if not isinstance(config["sanoid"], dict):
        raise ConfigError("sanoid must be a dictionary")
    
    if "enabled" not in config["sanoid"]:
        raise ConfigError("Missing required sanoid configuration key: enabled")
    
    if "config_path" not in config["sanoid"]:
        raise ConfigError("Missing required sanoid configuration key: config_path")
    
    # Validate saved_configurations
    if not isinstance(config["saved_configurations"], list):
        raise ConfigError("saved_configurations must be a list")

def add_saved_configuration(
    name: str,
    source_dataset: str,
    destination_server: str,
    destination_dataset: str,
    sync_options: Optional[Dict[str, Any]] = None
) -> None:
    """
    Add a saved configuration.
    
    Args:
        name: Name of the configuration
        source_dataset: Source dataset
        destination_server: Destination server
        destination_dataset: Destination dataset
        sync_options: Sync options
    """
    config = load_config()
    
    # Check if a configuration with the same name already exists
    for saved_config in config["saved_configurations"]:
        if saved_config["name"] == name:
            raise ConfigError(f"A configuration with the name '{name}' already exists")
    
    # Create the new configuration
    new_config = {
        "name": name,
        "source_dataset": source_dataset,
        "destination_server": destination_server,
        "destination_dataset": destination_dataset,
        "sync_options": sync_options or {}
    }
    
    # Add the new configuration to the list
    config["saved_configurations"].append(new_config)
    
    # Save the updated configuration
    save_config(config)
    
    logger.info(f"Saved configuration '{name}' added")

def remove_saved_configuration(name: str) -> None:
    """
    Remove a saved configuration.
    
    Args:
        name: Name of the configuration
    """
    config = load_config()
    
    # Find the configuration with the given name
    for i, saved_config in enumerate(config["saved_configurations"]):
        if saved_config["name"] == name:
            # Remove the configuration
            del config["saved_configurations"][i]
            
            # Save the updated configuration
            save_config(config)
            
            logger.info(f"Saved configuration '{name}' removed")
            return
    
    raise ConfigError(f"No configuration found with the name '{name}'")

def get_saved_configuration(name: str) -> Dict[str, Any]:
    """
    Get a saved configuration.
    
    Args:
        name: Name of the configuration
        
    Returns:
        Saved configuration dictionary
    """
    config = load_config()
    
    # Find the configuration with the given name
    for saved_config in config["saved_configurations"]:
        if saved_config["name"] == name:
            return saved_config
    
    raise ConfigError(f"No configuration found with the name '{name}'")

def list_saved_configurations() -> List[Dict[str, Any]]:
    """
    List all saved configurations.
    
    Returns:
        List of saved configuration dictionaries
    """
    config = load_config()
    return config["saved_configurations"]

def update_default_configuration(
    source_dataset: Optional[str] = None,
    destination_server: Optional[str] = None,
    destination_dataset: Optional[str] = None,
    sync_options: Optional[Dict[str, Any]] = None,
    sanoid: Optional[Dict[str, Any]] = None
) -> None:
    """
    Update the default configuration.
    
    Args:
        source_dataset: Source dataset
        destination_server: Destination server
        destination_dataset: Destination dataset
        sync_options: Sync options
        sanoid: Sanoid configuration
    """
    config = load_config()
    
    if source_dataset is not None:
        config["default_source_dataset"] = source_dataset
    
    if destination_server is not None:
        config["default_destination_server"] = destination_server
    
    if destination_dataset is not None:
        config["default_destination_dataset"] = destination_dataset
    
    if sync_options is not None:
        config["sync_options"].update(sync_options)
    
    if sanoid is not None:
        config["sanoid"].update(sanoid)
    
    save_config(config)
    
    logger.info("Default configuration updated")