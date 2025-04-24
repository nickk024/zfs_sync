"""
ZFS Operations Module

This module provides functions for interacting with ZFS datasets and snapshots.
"""

import logging
import subprocess
import re
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger('zfs_sync.core.zfs_ops')

class ZFSOperationError(Exception):
    """Exception raised for errors in ZFS operations."""
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
        ZFSOperationError: If the command fails and check is True
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
            raise ZFSOperationError(f"Command failed with exit code {process.returncode}: {stderr}")
        
        return stdout, stderr
    except Exception as e:
        logger.error(f"Error running command: {e}")
        raise ZFSOperationError(f"Error running command: {e}")

def list_datasets() -> List[str]:
    """
    List all ZFS datasets.
    
    Returns:
        List of dataset names
    """
    stdout, _ = run_command(['zfs', 'list', '-H', '-o', 'name'])
    return [line.strip() for line in stdout.splitlines() if line.strip()]

def list_snapshots(dataset: str) -> List[str]:
    """
    List all snapshots for a dataset.
    
    Args:
        dataset: Dataset name
        
    Returns:
        List of snapshot names
    """
    stdout, _ = run_command(['zfs', 'list', '-H', '-t', 'snapshot', '-o', 'name', '-r', dataset])
    return [line.strip() for line in stdout.splitlines() if line.strip()]

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
    run_command(['zfs', 'snapshot', full_snapshot_name])
    return full_snapshot_name

def send_snapshot(
    source_snapshot: str, 
    destination: str, 
    incremental_source: Optional[str] = None,
    resume_token: Optional[str] = None
) -> None:
    """
    Send a snapshot to a destination.
    
    Args:
        source_snapshot: Source snapshot name
        destination: Destination (dataset or file)
        incremental_source: Source snapshot for incremental send
        resume_token: Resume token for resuming interrupted transfer
        
    Raises:
        ZFSOperationError: If the send operation fails
    """
    command = ['zfs', 'send']
    
    if resume_token:
        command.extend(['-t', resume_token])
    elif incremental_source:
        command.extend(['-i', incremental_source, source_snapshot])
    else:
        command.append(source_snapshot)
    
    # If destination is a remote location, we'll handle it differently
    if '@' in destination:
        # Remote destination
        server, remote_dataset = destination.split(':', 1)
        send_process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        receive_command = ['ssh', server, 'zfs', 'receive', remote_dataset]
        receive_process = subprocess.Popen(
            receive_command,
            stdin=send_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        send_process.stdout.close()
        stdout, stderr = receive_process.communicate()
        
        if receive_process.returncode != 0:
            raise ZFSOperationError(f"Receive failed: {stderr}")
        
        if send_process.wait() != 0:
            _, send_stderr = send_process.communicate()
            raise ZFSOperationError(f"Send failed: {send_stderr.decode('utf-8')}")
    else:
        # Local destination
        command.extend(['|', 'zfs', 'receive', destination])
        # Use shell=True for pipe redirection
        process = subprocess.Popen(
            ' '.join(command),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise ZFSOperationError(f"Send/receive failed: {stderr}")

def get_resume_token() -> Optional[str]:
    """
    Get the resume token for the most recent interrupted transfer.
    
    Returns:
        Resume token or None if no interrupted transfer
    """
    try:
        stdout, _ = run_command(['zfs', 'get', '-H', '-o', 'value', 'receive_resume_token', 'pool'], check=False)
        token = stdout.strip()
        return token if token != '-' else None
    except ZFSOperationError:
        return None

def get_dataset_properties(dataset: str) -> Dict[str, str]:
    """
    Get properties of a dataset.
    
    Args:
        dataset: Dataset name
        
    Returns:
        Dictionary of property name to value
    """
    stdout, _ = run_command(['zfs', 'get', 'all', '-H', '-o', 'property,value', dataset])
    properties = {}
    
    for line in stdout.splitlines():
        if not line.strip():
            continue
        
        parts = line.split('\t')
        if len(parts) >= 2:
            properties[parts[0]] = parts[1]
    
    return properties

def dataset_exists(dataset: str) -> bool:
    """
    Check if a dataset exists.
    
    Args:
        dataset: Dataset name
        
    Returns:
        True if the dataset exists, False otherwise
    """
    try:
        run_command(['zfs', 'list', '-H', dataset])
        return True
    except ZFSOperationError:
        return False

def snapshot_exists(snapshot: str) -> bool:
    """
    Check if a snapshot exists.
    
    Args:
        snapshot: Snapshot name
        
    Returns:
        True if the snapshot exists, False otherwise
    """
    try:
        run_command(['zfs', 'list', '-H', '-t', 'snapshot', snapshot])
        return True
    except ZFSOperationError:
        return False