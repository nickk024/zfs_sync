#!/usr/bin/env python3
"""
ZFS Sync Tool - Entry point for direct execution

This module allows the package to be run directly using:
python -m zfs_sync
"""

from zfs_sync.main import main

if __name__ == "__main__":
    main()