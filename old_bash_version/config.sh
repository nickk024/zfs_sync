#!/bin/bash
set -euo pipefail # Exit on error, unset var, pipe failure
# ZFS Sync - Configuration Handling (Multi-Job Support)
# --------------------------------

# Get script directory for relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

# Declare Associative Arrays for Job Configurations (Requires Bash 4.0+)
declare -gA ZFS_SYNC_JOBS # Array to hold the names of valid jobs
declare -gA JOB_SOURCE_HOST
declare -gA JOB_SOURCE_DATASET
declare -gA JOB_DEST_HOST
declare -gA JOB_DEST_DATASET
declare -gA JOB_SSH_USER
declare -gA JOB_SNAPSHOT_PREFIX
declare -gA JOB_MAX_SNAPSHOTS
declare -gA JOB_RECURSIVE
declare -gA JOB_USE_COMPRESSION
declare -gA JOB_RESUME_SUPPORT
declare -gA JOB_DIRECT_REMOTE_TRANSFER
declare -gA JOB_SYNC_SNAPSHOT # Derived from prefix

load_config() {
  if [[ -f "$SCRIPT_DIR/.env" ]]; then
      set +e # Disable exit on error temporarily
      source "$SCRIPT_DIR/.env"
      local source_result=$?
      set -e # Re-enable exit on error
      if [[ $source_result -ne 0 ]]; then
          echo "Warning: Errors encountered while sourcing $SCRIPT_DIR/.env" >&2
      else
          echo "Loaded configuration from $SCRIPT_DIR/.env"
      fi
  else
      echo "Warning: No .env file found at $SCRIPT_DIR/.env - using defaults"
  fi

  local global_ssh_user=${DEFAULT_SSH_USER:-"root"}
  local global_snapshot_prefix=${DEFAULT_SNAPSHOT_PREFIX:-"backup"}
  local global_max_snapshots=${DEFAULT_MAX_SNAPSHOTS:-5}
  local global_recursive=${DEFAULT_RECURSIVE:-"true"}
  local global_use_compression=${DEFAULT_USE_COMPRESSION:-"true"}
  local global_resume_support=${DEFAULT_RESUME_SUPPORT:-"true"}
  local global_direct_remote_transfer=${DEFAULT_DIRECT_REMOTE_TRANSFER:-"false"}

  # Global settings that are not job-specific
  DEBUG_MODE=${DEBUG_MODE:-"false"}
  SSH_TIMEOUT=${SSH_TIMEOUT:-10}
  CMD_TIMEOUT=${CMD_TIMEOUT:-3600}

  # --- Logging Configuration (Global) ---
  LOG_DIR="$SCRIPT_DIR/logs"
  mkdir -p "$LOG_DIR"
  LOG_FILE="$LOG_DIR/zfs_sync_$(date +%Y%m%d-%H%M%S).log"
  # Initialize log file
  echo "=== ZFS Sync Log - Started $(date) ===" > "$LOG_FILE"

  # --- Parse Job Definitions ---
  if [[ -z "${ZFS_SYNC_JOB_NAMES:-}" ]]; then
      echo "Warning: ZFS_SYNC_JOB_NAMES is not defined in .env. No jobs to run." >&2
      # Optionally, could try to load legacy single-job config here, but sticking to new format.
      return 0 # No jobs defined, but not necessarily a fatal error yet.
  fi

  local job_name
  local valid_job_count=0
  for job_name in $ZFS_SYNC_JOB_NAMES; do
      echo "Loading configuration for job: $job_name"

      # Dynamically read job-specific variables, falling back to globals
      local src_host_var="ZFS_SYNC_JOB_${job_name}_SOURCE_HOST"
      local src_dataset_var="ZFS_SYNC_JOB_${job_name}_SOURCE_DATASET"
      local dest_host_var="ZFS_SYNC_JOB_${job_name}_DEST_HOST"
      local dest_dataset_var="ZFS_SYNC_JOB_${job_name}_DEST_DATASET"
      local ssh_user_var="ZFS_SYNC_JOB_${job_name}_SSH_USER"
      local snapshot_prefix_var="ZFS_SYNC_JOB_${job_name}_SNAPSHOT_PREFIX"
      local max_snapshots_var="ZFS_SYNC_JOB_${job_name}_MAX_SNAPSHOTS"
      local recursive_var="ZFS_SYNC_JOB_${job_name}_RECURSIVE"
      local use_compression_var="ZFS_SYNC_JOB_${job_name}_USE_COMPRESSION"
      local resume_support_var="ZFS_SYNC_JOB_${job_name}_RESUME_SUPPORT"
      local direct_remote_var="ZFS_SYNC_JOB_${job_name}_DIRECT_REMOTE_TRANSFER"

      # Read values using indirect expansion, providing defaults
      JOB_SOURCE_HOST[$job_name]="${!src_host_var:-}"
      JOB_SOURCE_DATASET[$job_name]="${!src_dataset_var:-}"
      JOB_DEST_HOST[$job_name]="${!dest_host_var:-}"
      JOB_DEST_DATASET[$job_name]="${!dest_dataset_var:-}"
      JOB_SSH_USER[$job_name]="${!ssh_user_var:-$global_ssh_user}"
      JOB_SNAPSHOT_PREFIX[$job_name]="${!snapshot_prefix_var:-$global_snapshot_prefix}"
      JOB_MAX_SNAPSHOTS[$job_name]="${!max_snapshots_var:-$global_max_snapshots}"
      JOB_RECURSIVE[$job_name]="${!recursive_var:-$global_recursive}"
      JOB_USE_COMPRESSION[$job_name]="${!use_compression_var:-$global_use_compression}"
      JOB_RESUME_SUPPORT[$job_name]="${!resume_support_var:-$global_resume_support}"
      JOB_DIRECT_REMOTE_TRANSFER[$job_name]="${!direct_remote_var:-$global_direct_remote_transfer}"
      JOB_SYNC_SNAPSHOT[$job_name]="${JOB_SNAPSHOT_PREFIX[$job_name]}-sync" # Derive sync snapshot name

      # --- Validate Mandatory Job Settings ---
      local job_valid=true
      if [[ -z "${JOB_SOURCE_HOST[$job_name]}" ]]; then echo "ERROR: Job '$job_name': Missing ZFS_SYNC_JOB_${job_name}_SOURCE_HOST" >&2; job_valid=false; fi
      if [[ -z "${JOB_SOURCE_DATASET[$job_name]}" ]]; then echo "ERROR: Job '$job_name': Missing ZFS_SYNC_JOB_${job_name}_SOURCE_DATASET" >&2; job_valid=false; fi
      if [[ -z "${JOB_DEST_HOST[$job_name]}" ]]; then echo "ERROR: Job '$job_name': Missing ZFS_SYNC_JOB_${job_name}_DEST_HOST" >&2; job_valid=false; fi
      if [[ -z "${JOB_DEST_DATASET[$job_name]}" ]]; then echo "ERROR: Job '$job_name': Missing ZFS_SYNC_JOB_${job_name}_DEST_DATASET" >&2; job_valid=false; fi

      if [[ "$job_valid" == "true" ]]; then
          ZFS_SYNC_JOBS[$job_name]=1 # Mark job as validly configured
          ((valid_job_count++))
          echo "Successfully loaded configuration for job: $job_name"
      else
          echo "Skipping job '$job_name' due to missing mandatory configuration." >&2
          # Optionally remove invalid job entries from arrays here, but skipping is simpler.
      fi
      echo "---" # Separator between jobs
  done

  if [[ $valid_job_count -eq 0 ]]; then
      echo "Warning: No valid jobs were configured." >&2
  fi

  # --- Export Global Settings ---
  # Job-specific settings are now in the associative arrays and accessed directly by the main script
  export DEBUG_MODE SSH_TIMEOUT CMD_TIMEOUT
  export LOG_DIR LOG_FILE SCRIPT_DIR

  # Deprecated single-job exports (remove or comment out)
  # export DEFAULT_SOURCE_HOST DEFAULT_DEST_HOST DEFAULT_SSH_USER ... etc
}
