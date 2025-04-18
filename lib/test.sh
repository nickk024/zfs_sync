#!/bin/bash
# ZFS Sync - Test Functions
# --------------------------------

# Function to enable dry run mode - all functions will simulate operations without making changes
enable_dry_run() {
  log "Enabling dry run mode - no actual ZFS operations will be performed"
  DRY_RUN=true
  export DRY_RUN
  
  # Override the ZFS commands with mock versions
  override_commands
}

# Override commands with test versions
override_commands() {
  # Original function backups (using unique names to avoid conflicts)
  if [[ "$DRY_RUN" == "true" ]]; then
    # Save original functions with _real suffix
    if ! declare -f create_snapshot_real > /dev/null; then
      log "Setting up function overrides for dry run mode"
      eval "$(declare -f create_snapshot | sed 's/create_snapshot/create_snapshot_real/')"
      eval "$(declare -f get_snapshot | sed 's/get_snapshot/get_snapshot_real/')"
      eval "$(declare -f has_dataset | sed 's/has_dataset/has_dataset_real/')"
      eval "$(declare -f perform_full_transfer | sed 's/perform_full_transfer/perform_full_transfer_real/')"
      eval "$(declare -f perform_incremental_transfer | sed 's/perform_incremental_transfer/perform_incremental_transfer_real/')"
      eval "$(declare -f setup_sync_snapshot | sed 's/setup_sync_snapshot/setup_sync_snapshot_real/')"
      eval "$(declare -f clean_old_snapshots | sed 's/clean_old_snapshots/clean_old_snapshots_real/')"

      # Override functions with test versions
      create_snapshot() {
        local host=$1
        local dataset=$2
        local snapshot=$3
        local recursive=$4
        local ssh_user=$5
        
        log "[DRY RUN] Would create snapshot: $dataset@$snapshot on $host (recursive: $recursive)"
        return 0
      }
      
      get_snapshot() {
        local host=$1
        local dataset=$2
        local snapshot=$3
        local ssh_user=$4
        
        # For testing, we'll pretend snapshots with "sync" or "exists" in the name exist
        if [[ "$snapshot" == *"sync"* ]] || [[ "$snapshot" == *"exists"* ]]; then
          log "[DRY RUN] Found snapshot: $dataset@$snapshot on $host"
          return 0
        else
          log "[DRY RUN] Snapshot not found: $dataset@$snapshot on $host"
          return 1
        fi
      }
      
      has_dataset() {
        local host=$1
        local dataset=$2
        local ssh_user=$3
        
        # For testing, pretend all datasets exist
        log "[DRY RUN] Dataset $dataset exists on $host"
        return 0
      }
      
      perform_full_transfer() {
        local src_host=$1
        local src_dataset=$2
        local dst_host=$3
        local dst_dataset=$4
        local src_snapshot=$5
        local ssh_user=$6
        local recursive=$7
        
        log "[DRY RUN] Would perform full transfer from $src_host:$src_dataset@$src_snapshot to $dst_host:$dst_dataset"
        log "[DRY RUN] Transfer parameters: recursive=$recursive, compression=$USE_COMPRESSION, resume=$RESUME_SUPPORT"
        sleep 2  # Simulate some processing time
        return 0
      }
      
      perform_incremental_transfer() {
        local src_host=$1
        local src_dataset=$2
        local dst_host=$3
        local dst_dataset=$4
        local src_snapshot=$5
        local src_prev_snapshot=$6
        local ssh_user=$7
        local recursive=$8
        
        log "[DRY RUN] Would perform incremental transfer from $src_host:$src_dataset"
        log "[DRY RUN] Using snapshots: $src_dataset@$src_prev_snapshot → $src_dataset@$src_snapshot"
        log "[DRY RUN] To destination: $dst_host:$dst_dataset"
        log "[DRY RUN] Transfer parameters: recursive=$recursive, compression=$USE_COMPRESSION"
        sleep 2  # Simulate some processing time
        return 0
      }
      
      setup_sync_snapshot() {
        local src_host=$1
        local src_dataset=$2
        local dst_host=$3
        local dst_dataset=$4
        local snapshot=$5
        local sync_snapshot=$6
        local ssh_user=$7
        local recursive=$8
        local create_new=$9
        
        log "[DRY RUN] Would setup sync snapshots on both hosts"
        log "[DRY RUN] Source: $src_host:$src_dataset@$sync_snapshot"
        log "[DRY RUN] Destination: $dst_host:$dst_dataset@$sync_snapshot"
        log "[DRY RUN] Create new: $create_new, recursive: $recursive"
        return 0
      }
      
      clean_old_snapshots() {
        local dataset=$1
        local prefix=$2
        local keep=$3
        local host=$4
        local ssh_user=$5
        
        log "[DRY RUN] Would clean old snapshots on $host:$dataset with prefix $prefix (keeping $keep newest)"
        return 0
      }
    fi
  fi
}

# Restore original functions
restore_commands() {
  if [[ "$DRY_RUN" == "true" ]]; then
    log "Restoring original function implementations"
    if declare -f create_snapshot_real > /dev/null; then
      # Restore original functions
      eval "$(declare -f create_snapshot_real | sed 's/create_snapshot_real/create_snapshot/')"
      eval "$(declare -f get_snapshot_real | sed 's/get_snapshot_real/get_snapshot/')"
      eval "$(declare -f has_dataset_real | sed 's/has_dataset_real/has_dataset/')"
      eval "$(declare -f perform_full_transfer_real | sed 's/perform_full_transfer_real/perform_full_transfer/')"
      eval "$(declare -f perform_incremental_transfer_real | sed 's/perform_incremental_transfer_real/perform_incremental_transfer/')"
      eval "$(declare -f setup_sync_snapshot_real | sed 's/setup_sync_snapshot_real/setup_sync_snapshot/')"
      eval "$(declare -f clean_old_snapshots_real | sed 's/clean_old_snapshots_real/clean_old_snapshots/')"
      
      # Unset the backup functions
      unset -f create_snapshot_real
      unset -f get_snapshot_real
      unset -f has_dataset_real
      unset -f perform_full_transfer_real
      unset -f perform_incremental_transfer_real
      unset -f setup_sync_snapshot_real
      unset -f clean_old_snapshots_real
    fi
  fi
  
  # Disable dry run mode
  DRY_RUN=false
  export DRY_RUN
}

# Find common snapshots (special implementation for dry run)
find_common_snapshots_test() {
  log "[DRY RUN] Simulating common snapshot search"
  
  # For testing, return simulated common snapshots
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "backup-20230101-120000"
    echo "backup-20230201-130000"
  fi
}
