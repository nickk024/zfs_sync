"""
Sanoid Operations Module

This module provides functions for interacting with sanoid for snapshot management.
"""

import logging
import subprocess
import os
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union

logger = logging.getLogger('zfs_sync.core.sanoid_ops')

class SanoidOperationError(Exception):
    """Exception raised for errors in Sanoid operations."""
    pass

def run_command(command: List[str], check: bool = True) -> Tuple[str, str]:
    """
    Run a command and return its output.
    
    Args:
        command: List of command and arguments
        check: Whether to check the return code
        
    Returns:
        Tuple of (stdout, stderr)
        
    Raises:
        SanoidOperationError: If the command fails and check is True
    """
    logger.debug(f"Running command: {' '.join(command)}")
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        if check and process.returncode != 0:
            raise SanoidOperationError(f"Command failed with exit code {process.returncode}: {stderr}")
        
        return stdout, stderr
    except Exception as e:
        logger.error(f"Error running command: {e}")
        raise SanoidOperationError(f"Error running command: {e}")

def get_sanoid_path() -> str:
    """
    Get the path to the sanoid script.
    
    Returns:
        Path to the sanoid script
    """
    # Check if sanoid is in the current directory
    current_dir = Path.cwd()
    sanoid_path = current_dir / "libs" / "sanoid" / "sanoid"
    
    if sanoid_path.exists():
        return str(sanoid_path)
    
    # Check if sanoid is in the PATH
    try:
        stdout, _ = run_command(["which", "sanoid"])
        return stdout.strip()
    except SanoidOperationError:
        # If not found, use the default path
        return "/usr/local/bin/sanoid"

def get_syncoid_path() -> str:
    """
    Get the path to the syncoid script.
    
    Returns:
        Path to the syncoid script
    """
    # Check if syncoid is in the current directory
    current_dir = Path.cwd()
    syncoid_path = current_dir / "libs" / "sanoid" / "syncoid"
    
    if syncoid_path.exists():
        return str(syncoid_path)
    
    # Check if syncoid is in the PATH
    try:
        stdout, _ = run_command(["which", "syncoid"])
        return stdout.strip()
    except SanoidOperationError:
        # If not found, use the default path
        return "/usr/local/bin/syncoid"

def create_sanoid_config(config_path: str, datasets: Dict[str, Dict[str, Union[str, int]]]) -> None:
    """
    Create a sanoid configuration file.
    
    Args:
        config_path: Path to the configuration file
        datasets: Dictionary of datasets and their configuration
    """
    with open(config_path, 'w') as f:
        for dataset, config in datasets.items():
            f.write(f"[{dataset}]\n")
            for key, value in config.items():
                f.write(f"\t{key} = {value}\n")
            f.write("\n")
        
        # Add template section
        f.write("[template_production]\n")
        f.write("\tfrequently = 0\n")
        f.write("\thourly = 36\n")
        f.write("\tdaily = 30\n")
        f.write("\tmonthly = 3\n")
        f.write("\tyearly = 0\n")
        f.write("\tautosnap = yes\n")
        f.write("\tautoprune = yes\n")

def take_snapshots(config_path: Optional[str] = None) -> None:
    """
    Take snapshots using sanoid.
    
    Args:
        config_path: Path to the sanoid configuration file
    """
    sanoid_path = get_sanoid_path()
    
    command = [sanoid_path, "--take-snapshots"]
    if config_path:
        command.extend(["--configdir", os.path.dirname(config_path)])
    
    try:
        stdout, stderr = run_command(command)
        logger.info("Snapshots created successfully")
        return stdout
    except SanoidOperationError as e:
        logger.error(f"Failed to take snapshots: {e}")
        raise

def prune_snapshots(config_path: Optional[str] = None) -> None:
    """
    Prune snapshots using sanoid.
    
    Args:
        config_path: Path to the sanoid configuration file
    """
    sanoid_path = get_sanoid_path()
    
    command = [sanoid_path, "--prune-snapshots"]
    if config_path:
        command.extend(["--configdir", os.path.dirname(config_path)])
    
    try:
        stdout, stderr = run_command(command)
        logger.info("Snapshots pruned successfully")
        return stdout
    except SanoidOperationError as e:
        logger.error(f"Failed to prune snapshots: {e}")
        raise

def sync_dataset(source: str, target: str, options: Optional[Dict[str, Union[str, bool]]] = None) -> None:
    """
    Sync a dataset using syncoid.
    
    Args:
        source: Source dataset
        target: Target dataset
        options: Dictionary of options to pass to syncoid
    """
    syncoid_path = get_syncoid_path()
    
    command = [syncoid_path]
    
    if options:
        for key, value in options.items():
            if isinstance(value, bool):
                if value:
                    command.append(f"--{key}")
            else:
                command.append(f"--{key}={value}")
    
    command.extend([source, target])
    
    try:
        stdout, stderr = run_command(command)
        logger.info(f"Dataset {source} synced to {target} successfully")
        return stdout
    except SanoidOperationError as e:
        logger.error(f"Failed to sync dataset {source} to {target}: {e}")
        raise

def list_snapshots(dataset: str) -> List[Dict[str, str]]:
    """
    List snapshots for a dataset.
    
    Args:
        dataset: Dataset name
        
    Returns:
        List of snapshots with their properties
    """
    try:
        stdout, _ = run_command(["zfs", "list", "-H", "-t", "snapshot", "-o", "name,creation", "-r", dataset])
        
        snapshots = []
        for line in stdout.splitlines():
            if not line.strip():
                continue
            
            parts = line.split('\t')
            if len(parts) >= 2:
                name = parts[0]
                creation = parts[1]
                
                # Extract snapshot name from full path
                snapshot_name = name.split('@')[1] if '@' in name else name
                
                snapshots.append({
                    'name': name,
                    'snapshot_name': snapshot_name,
                    'creation': creation
                })
        
        return snapshots
    except SanoidOperationError as e:
        logger.error(f"Failed to list snapshots for dataset {dataset}: {e}")
        raise

def get_latest_snapshot(dataset: str) -> Optional[str]:
    """
    Get the latest snapshot for a dataset.
    
    Args:
        dataset: Dataset name
        
    Returns:
        Latest snapshot name or None if no snapshots exist
    """
    try:
        snapshots = list_snapshots(dataset)
        if not snapshots:
            return None
        
        # Sort by creation time (newest first)
        snapshots.sort(key=lambda x: x['creation'], reverse=True)
        
        return snapshots[0]['name']
    except SanoidOperationError as e:
        logger.error(f"Failed to get latest snapshot for dataset {dataset}: {e}")
        raise

def find_matching_snapshots(source_dataset: str, target_dataset: str) -> List[str]:
    """
    Find snapshots that exist on both source and target datasets.
    
    Args:
        source_dataset: Source dataset name
        target_dataset: Target dataset name
        
    Returns:
        List of matching snapshot names
    """
    try:
        source_snapshots = list_snapshots(source_dataset)
        target_snapshots = list_snapshots(target_dataset)
        
        source_snapshot_names = [s['snapshot_name'] for s in source_snapshots]
        target_snapshot_names = [s['snapshot_name'] for s in target_snapshots]
        
        matching_snapshots = list(set(source_snapshot_names) & set(target_snapshot_names))
        
        return matching_snapshots
    except SanoidOperationError as e:
        logger.error(f"Failed to find matching snapshots: {e}")
        raise

def create_snapshot(dataset: str, snapshot_name: str) -> str:
    """
    Create a snapshot of a dataset.
    
    Args:
        dataset: Dataset name
        snapshot_name: Snapshot name
        
    Returns:
        Full snapshot name (dataset@snapshot_name)
    """
    full_snapshot_name = f"{dataset}@{snapshot_name}"
    
    try:
        run_command(["zfs", "snapshot", full_snapshot_name])
        logger.info(f"Snapshot {full_snapshot_name} created successfully")
        return full_snapshot_name
    except SanoidOperationError as e:
        logger.error(f"Failed to create snapshot {full_snapshot_name}: {e}")
        raise

def delete_snapshot(snapshot: str) -> None:
    """
    Delete a snapshot.
    
    Args:
        snapshot: Full snapshot name (dataset@snapshot_name)
    """
    try:
        run_command(["zfs", "destroy", snapshot])
        logger.info(f"Snapshot {snapshot} deleted successfully")
    except SanoidOperationError as e:
        logger.error(f"Failed to delete snapshot {snapshot}: {e}")
        raise

def create_default_sanoid_config(config_path: str, dataset: str) -> None:
    """
    Create a default sanoid configuration file for a dataset.
    
    Args:
        config_path: Path to the configuration file
        dataset: Dataset name
    """
    datasets = {
        dataset: {
            "use_template": "production"
        }
    }
    
    create_sanoid_config(config_path, datasets)
    logger.info(f"Default sanoid configuration created at {config_path}")