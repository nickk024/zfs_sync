"""
SSH Operations Module

This module provides functions for interacting with remote servers via SSH.
"""

import logging
import subprocess
import os
import paramiko
from typing import List, Dict, Optional, Tuple, Union, Callable

logger = logging.getLogger('zfs_sync.core.ssh_ops')

class SSHOperationError(Exception):
    """Exception raised for errors in SSH operations."""
    pass

class SSHConnection:
    """Class for managing SSH connections."""
    
    def __init__(
        self, 
        hostname: str, 
        username: Optional[str] = None,
        port: int = 22,
        key_filename: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize SSH connection.
        
        Args:
            hostname: Remote hostname or IP
            username: SSH username (defaults to current user if None)
            port: SSH port
            key_filename: Path to private key file
            password: Password for authentication (if not using key)
        """
        self.hostname = hostname
        self.username = username or os.environ.get('USER')
        self.port = port
        self.key_filename = key_filename
        self.password = password
        self.client = None
        
        logger.debug(f"Initialized SSH connection to {self.username}@{self.hostname}:{self.port}")
    
    def connect(self) -> None:
        """
        Establish SSH connection.
        
        Raises:
            SSHOperationError: If connection fails
        """
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_kwargs = {
                'hostname': self.hostname,
                'port': self.port,
                'username': self.username,
            }
            
            if self.key_filename:
                connect_kwargs['key_filename'] = self.key_filename
            elif self.password:
                connect_kwargs['password'] = self.password
            
            self.client.connect(**connect_kwargs)
            logger.debug(f"Connected to {self.username}@{self.hostname}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.username}@{self.hostname}:{self.port}: {e}")
            raise SSHOperationError(f"Failed to connect: {e}")
    
    def disconnect(self) -> None:
        """Close SSH connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.debug(f"Disconnected from {self.username}@{self.hostname}:{self.port}")
    
    def execute_command(self, command: str) -> Tuple[str, str, int]:
        """
        Execute command on remote server.
        
        Args:
            command: Command to execute
            
        Returns:
            Tuple of (stdout, stderr, exit_code)
            
        Raises:
            SSHOperationError: If command execution fails
        """
        if not self.client:
            self.connect()
        
        try:
            logger.debug(f"Executing command on {self.hostname}: {command}")
            stdin, stdout, stderr = self.client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            
            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')
            
            logger.debug(f"Command exit code: {exit_code}")
            return stdout_str, stderr_str, exit_code
        except Exception as e:
            logger.error(f"Failed to execute command: {e}")
            raise SSHOperationError(f"Failed to execute command: {e}")
    
    def check_zfs_installed(self) -> bool:
        """
        Check if ZFS is installed on the remote server.
        
        Returns:
            True if ZFS is installed, False otherwise
        """
        try:
            stdout, stderr, exit_code = self.execute_command("which zfs")
            return exit_code == 0
        except SSHOperationError:
            return False
    
    def list_remote_datasets(self) -> List[str]:
        """
        List ZFS datasets on the remote server.
        
        Returns:
            List of dataset names
            
        Raises:
            SSHOperationError: If command execution fails
        """
        stdout, stderr, exit_code = self.execute_command("zfs list -H -o name")
        
        if exit_code != 0:
            raise SSHOperationError(f"Failed to list remote datasets: {stderr}")
        
        return [line.strip() for line in stdout.splitlines() if line.strip()]
    
    def check_dataset_exists(self, dataset: str) -> bool:
        """
        Check if a dataset exists on the remote server.
        
        Args:
            dataset: Dataset name
            
        Returns:
            True if the dataset exists, False otherwise
        """
        try:
            stdout, stderr, exit_code = self.execute_command(f"zfs list -H {dataset}")
            return exit_code == 0
        except SSHOperationError:
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

def test_ssh_connection(
    hostname: str, 
    username: Optional[str] = None,
    port: int = 22,
    key_filename: Optional[str] = None,
    password: Optional[str] = None
) -> bool:
    """
    Test SSH connection to a remote server.
    
    Args:
        hostname: Remote hostname or IP
        username: SSH username (defaults to current user if None)
        port: SSH port
        key_filename: Path to private key file
        password: Password for authentication (if not using key)
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with SSHConnection(
            hostname=hostname,
            username=username,
            port=port,
            key_filename=key_filename,
            password=password
        ) as ssh:
            # Just connecting is enough to test
            return True
    except SSHOperationError:
        return False

def get_known_hosts() -> List[str]:
    """
    Get list of known hosts from SSH known_hosts file.
    
    Returns:
        List of hostnames
    """
    known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")
    
    if not os.path.exists(known_hosts_file):
        return []
    
    hosts = []
    
    try:
        with open(known_hosts_file, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    # Extract hostname from known_hosts line
                    parts = line.strip().split(' ', 1)[0].split(',')
                    hosts.extend(parts)
    except Exception as e:
        logger.error(f"Failed to read known_hosts file: {e}")
    
    return hosts