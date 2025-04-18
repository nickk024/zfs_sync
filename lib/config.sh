#!/bin/bash
# ZFS Sync - Configuration Handling
# --------------------------------

# Get script directory for relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

# Load and apply configuration
load_config() {
  # Load environment variables from .env file if it exists
  if [[ -f "$SCRIPT_DIR/.env" ]]; then
      source "$SCRIPT_DIR/.env"
      echo "Loaded configuration from $SCRIPT_DIR/.env"
  else
      echo "Warning: No .env file found at $SCRIPT_DIR/.env - using defaults"
  fi

  # Default settings - can be overridden interactively or via .env file
  DEFAULT_SOURCE_HOST=${DEFAULT_SOURCE_HOST:-"local"}
  DEFAULT_DEST_HOST=${DEFAULT_DEST_HOST:-""}
  DEFAULT_SSH_USER=${DEFAULT_SSH_USER:-"root"}
  SNAPSHOT_PREFIX=${SNAPSHOT_PREFIX:-"backup"}
  SYNC_SNAPSHOT="${SNAPSHOT_PREFIX}-sync"
  MAX_SNAPSHOTS=${MAX_SNAPSHOTS:-5}  # How many snapshots to keep on source and destination
  DEBUG_MODE=${DEBUG_MODE:-"false"}  # Set to true for additional debugging output

  # Advanced options
  USE_COMPRESSION=${USE_COMPRESSION:-true}    # Whether to use compression in transit
  VERIFY_TRANSFERS=${VERIFY_TRANSFERS:-false} # Whether to verify transfers (slows things down but adds security)
  RESUME_SUPPORT=${RESUME_SUPPORT:-true}      # Whether to enable resumable transfers (needs mbuffer)

  # Timeout settings (in seconds)
  SSH_TIMEOUT=${SSH_TIMEOUT:-10}              # Timeout for SSH connections
  CMD_TIMEOUT=${CMD_TIMEOUT:-3600}            # Timeout for long-running commands (1 hour default)

  # Logging configuration
  LOG_DIR="$SCRIPT_DIR/logs"
  mkdir -p "$LOG_DIR"
  LOG_FILE="$LOG_DIR/zfs_sync_$(date +%Y%m%d-%H%M%S).log"  # Log file with timestamp

  # Initialize log file
  echo "=== ZFS Sync Log - Started $(date) ===" > "$LOG_FILE"
  
  # Export variables to be available globally
  export DEFAULT_SOURCE_HOST DEFAULT_DEST_HOST DEFAULT_SSH_USER
  export SNAPSHOT_PREFIX SYNC_SNAPSHOT MAX_SNAPSHOTS DEBUG_MODE
  export USE_COMPRESSION VERIFY_TRANSFERS RESUME_SUPPORT
  export SSH_TIMEOUT CMD_TIMEOUT
  export LOG_DIR LOG_FILE SCRIPT_DIR
}
