#!/bin/bash
# ZFS Sync - Dataset Management Functions
# --------------------------------

# Get list of datasets from host
get_datasets() {
  local host=$1
  local ssh_user=$2
  local max_count=${3:-10}  # Default to 10 datasets
  
  debug "Fetching datasets from $host (max: $max_count)"
  
  if [[ "$host" == "local" ]]; then
    zfs list -H -o name 2>/dev/null | head -n "$max_count"
  else
    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs list -H -o name 2>/dev/null | head -n $max_count"
  fi
}

# Check if dataset exists on host
has_dataset() {
  local host=$1
  local dataset=$2
  local ssh_user=$3
  
  debug "Checking if dataset $dataset exists on $host"
  
  if [[ "$host" == "local" ]]; then
    zfs list "$dataset" >/dev/null 2>&1
    local result=$?
    debug "Local dataset check result: $result (0=exists, 1=not found)"
    return $result
  else
    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs list '$dataset'" >/dev/null 2>&1
    local result=$?
    debug "Remote dataset check result: $result (0=exists, 1=not found)"
    return $result
  fi
}

# Check if snapshot exists on host
get_snapshot() {
  local host=$1
  local dataset=$2
  local snapshot=$3
  local ssh_user=$4
  
  debug "Checking for snapshot $dataset@$snapshot on $host"
  
  if [[ "$host" == "local" ]]; then
    zfs list -t snapshot -o name "$dataset@$snapshot" >/dev/null 2>&1
    local result=$?
    debug "Local snapshot check result: $result (0=exists, 1=not found)"
    return $result
  else
    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs list -t snapshot -o name '$dataset@$snapshot'" >/dev/null 2>&1
    local result=$?
    debug "Remote snapshot check result: $result (0=exists, 1=not found)"
    return $result
  fi
}

# Find common snapshots between source and destination
# IMPORTANT: This function should only echo the list of common snapshot names to stdout.
# All logging must go to stderr (using log, warn, error, debug).
find_common_snapshots() {
  local src_host=$1
  local src_dataset=$2
  local dst_host=$3
  local dst_dataset=$4
  local ssh_user=$5

  log "Checking for common snapshots between source and destination..." >&2 # Log to stderr

  # Get source snapshots
  local src_snaps=""
  if [[ "$src_host" == "local" ]]; then
    debug "Getting snapshots for local source dataset $src_dataset"
    src_snaps=$(zfs list -t snapshot -o name -H "$src_dataset" 2>/dev/null | awk -F@ '{print $2}' | sort || echo "")
  else
    debug "Getting snapshots for remote source dataset $src_dataset on $src_host"
    src_snaps=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$src_host" "zfs list -t snapshot -o name -H '$src_dataset' 2>/dev/null" | awk -F@ '{print $2}' | sort || echo "")
  fi
  
  # Get destination snapshots
  local dst_snaps=""
  if [[ "$dst_host" == "local" ]]; then
    debug "Getting snapshots for local destination dataset $dst_dataset"
    dst_snaps=$(zfs list -t snapshot -o name -H "$dst_dataset" 2>/dev/null | awk -F@ '{print $2}' | sort || echo "")
  else
    debug "Getting snapshots for remote destination dataset $dst_dataset on $dst_host"
    dst_snaps=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$dst_host" "zfs list -t snapshot -o name -H '$dst_dataset' 2>/dev/null" | awk -F@ '{print $2}' | sort || echo "")
  fi
  
  debug "Source snapshots: $(echo "$src_snaps" | tr '\n' ' ')" >&2 # Log to stderr
  debug "Destination snapshots: $(echo "$dst_snaps" | tr '\n' ' ')" >&2 # Log to stderr

  # Find common snapshots - make sure they are properly trimmed to avoid invalid names
  local common_snaps_list=""
  for snap in $src_snaps; do
    # Trim the snapshot name to remove any potential whitespace or newlines
    snap=$(echo "$snap" | tr -d '\n\r')
    
    # Check if this snapshot exists in destination snapshots
    if echo "$dst_snaps" | grep -q "^$snap\$"; then
      common_snaps_list="$common_snaps_list$snap\n"
      debug "Found common snapshot: $snap" >&2 # Log to stderr
    fi
  done

  # Sort common snapshots by time (assuming they contain timestamps) and echo ONLY the list to stdout
  if [[ -n "$common_snaps_list" ]]; then
    echo -e "$common_snaps_list" | sort -r # Output sorted list to stdout
    local count=$(echo -e "$common_snaps_list" | grep -v "^$" | wc -l)
    debug "Found $count common snapshots" >&2 # Log count to stderr
  else
    debug "No common snapshots found" >&2 # Log to stderr
    # Output nothing to stdout if no common snapshots found
  fi
}

# Create a ZFS snapshot
create_snapshot() {
  local host=$1
  local dataset=$2
  local snapshot=$3
  local recursive=$4
  local ssh_user=$5
  
  local r_flag=""
  if [[ "$recursive" == "true" ]]; then
    r_flag="-r"
  fi
  
  # Check if the snapshot already exists
  if get_snapshot "$host" "$dataset" "$snapshot" "$ssh_user"; then
    warn "Snapshot $dataset@$snapshot already exists on $host. Skipping creation."
    return 0
  fi
  
  log "Creating snapshot $dataset@$snapshot on $host"
  if [[ "$host" == "local" ]]; then
    zfs snapshot $r_flag "$dataset@$snapshot" || error "Failed to create snapshot $dataset@$snapshot"
  else
    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs snapshot $r_flag '$dataset@$snapshot'" || error "Failed to create snapshot $dataset@$snapshot on $host"
  fi
  
  # Verify snapshot was created
  if ! get_snapshot "$host" "$dataset" "$snapshot" "$ssh_user"; then
    error "Verification failed: Snapshot $dataset@$snapshot was not successfully created on $host"
  fi
  
  log "Successfully created and verified snapshot $dataset@$snapshot on $host"
}

# Clean up old snapshots, keeping only the newest ones
clean_old_snapshots() {
  local dataset=$1
  local prefix=$2
  local keep=$3
  local host=$4
  local ssh_user=$5
  
  log "Cleaning old snapshots with prefix $prefix on $host (keeping newest $keep)..."
  
  local snapshots=""
  if [[ "$host" == "local" ]]; then
    snapshots=$(zfs list -t snapshot -o name -H | grep "$dataset@$prefix" | sort)
  else
    snapshots=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs list -t snapshot -o name -H | grep '$dataset@$prefix' | sort")
  fi
  
  # Count total snapshots with this prefix
  local total_snaps=$(echo "$snapshots" | grep -c .)
  debug "Found $total_snaps snapshots with prefix $prefix for dataset $dataset on $host"
  
  if [[ $total_snaps -le $keep ]]; then
    log "Only $total_snaps snapshots exist with prefix $prefix for $dataset on $host. None will be removed."
    return 0
  fi
  
  # Calculate how many snapshots to remove
  local to_remove=$((total_snaps - keep))
  log "Will remove $to_remove oldest snapshots"
  
  # Get list of snapshots to remove
  local snapshots_to_remove=$(echo "$snapshots" | head -n $to_remove)
  
  if [[ -z "$snapshots_to_remove" ]]; then
    debug "No snapshots to remove"
    return 0
  fi
  
  # Remove each snapshot individually for better error handling
  local success_count=0
  local fail_count=0
  
  while read -r snap; do
    if [[ -n "$snap" ]]; then
      debug "Removing snapshot: $snap"
      
      if [[ "$host" == "local" ]]; then
        if zfs destroy "$snap" 2>/dev/null; then
          ((success_count++))
          debug "Successfully removed $snap"
        else
          ((fail_count++))
          warn "Failed to remove snapshot $snap"
        fi
      else
        if ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs destroy '$snap'" 2>/dev/null; then
          ((success_count++))
          debug "Successfully removed $snap"
        else
          ((fail_count++))
          warn "Failed to remove snapshot $snap"
        fi
      fi
    fi
  done <<< "$snapshots_to_remove"
  
  log "Snapshot cleanup summary: Removed $success_count snapshots, $fail_count failed"
}

# Check and clean up incomplete snapshots
cleanup_incomplete_snapshots() {
  local host=$1
  local dataset=$2
  local pattern=$3
  local ssh_user=$4
  local recursive=$5
  
  log "Checking for potentially incomplete snapshots on $host:$dataset matching pattern $pattern..."
  
  local snapshot_list
  if [[ "$host" == "local" ]]; then
    snapshot_list=$(zfs list -t snapshot -o name -H "$dataset" 2>/dev/null | grep "@$pattern" || echo "")
  else
    snapshot_list=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs list -t snapshot -o name -H '$dataset' 2>/dev/null | grep '@$pattern'" || echo "")
  fi
  
  if [[ -n "$snapshot_list" ]]; then
    log "Found potentially incomplete snapshots. Cleaning up..."
    
    local r_flag=""
    [[ "$recursive" == "true" ]] && r_flag="-r"
    
    while read -r snapshot; do
      if [[ -n "$snapshot" ]]; then
        debug "Removing incomplete snapshot: $snapshot"
        if [[ "$host" == "local" ]]; then
          zfs destroy $r_flag "$snapshot" 2>/dev/null && log "Removed incomplete snapshot: $snapshot" || warn "Failed to remove snapshot $snapshot"
        else
          ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "zfs destroy $r_flag '$snapshot'" 2>/dev/null && log "Removed incomplete snapshot: $snapshot" || warn "Failed to remove snapshot $snapshot"
        fi
      fi
    done <<< "$snapshot_list"
  else
    debug "No incomplete snapshots found"
  fi
}

# Function to confirm dataset exists on both source and destination
# Returns 0 if destination exists (or will be created), 1 otherwise.
# All logging goes to stderr.
confirm_dataset_exists() {
  local src_host=$1
  local src_dataset=$2
  local dst_host=$3
  local dst_dataset=$4
  local ssh_user=$5
  local create_if_missing=$6
  local recursive_flag=$RECURSIVE # Need to pass recursive flag for cleanup

  log "Checking if dataset exists on source and destination..." >&2 # Log to stderr

  # Check source
  if ! has_dataset "$src_host" "$src_dataset" "$ssh_user"; then
    error "Source dataset $src_dataset does not exist on $src_host." # error already goes to stderr
  else
    log "Source dataset confirmed: $src_dataset on $src_host" >&2 # Log to stderr
  fi

  # Check destination
  if ! has_dataset "$dst_host" "$dst_dataset" "$ssh_user"; then
    log "Destination dataset $dst_dataset doesn't exist on $dst_host." >&2 # Log to stderr
    if [[ "$create_if_missing" == "true" ]]; then
      log "Will create destination dataset during transfer." >&2 # Log to stderr
      return 0 # Treat as "exists" because it will be created
    else
      error "Destination dataset doesn't exist and auto-creation is disabled." # error already goes to stderr
      # Error function exits, but for clarity:
      return 1 # Destination does not exist and won't be created
    fi
  else
    log "Destination dataset confirmed: $dst_dataset on $dst_host (this may have been created by other means)" >&2 # Log to stderr

    # Check for any interrupted/incomplete snapshots and clean them up
    # Logging within cleanup_incomplete_snapshots should already go to stderr via log/warn/debug
    cleanup_incomplete_snapshots "$dst_host" "$dst_dataset" "initial-sync" "$ssh_user" "$recursive_flag" >&2
    cleanup_incomplete_snapshots "$dst_host" "$dst_dataset" "tmp" "$ssh_user" "$recursive_flag" >&2
    return 0 # Destination exists
  fi
}

# Interactive dataset selection
select_dataset() {
  local host=$1
  local prompt=$2
  local default=$3
  local ssh_user=$4
  
  echo "$prompt"
  
  local datasets
  datasets=$(get_datasets "$host" "$ssh_user")
  
  # Display numbered list of datasets
  local i=1
  local dataset_array=()
  
  echo "Available datasets:"
  while read -r dataset; do
    if [[ -n "$dataset" ]]; then
      printf "  %2d) %s\n" $i "$dataset"
      dataset_array+=("$dataset")
      ((i++))
    fi
  done <<< "$datasets"
  
  echo "  M) Enter dataset name manually"
  
  # Get user selection
  echo -n "Select dataset [${default:-required}]: "
  read -r selected
  
  # Handle manual entry option
  if [[ "${selected,,}" == "m" ]]; then
    echo -n "Enter dataset name: "
    read -r selected
  elif [[ -z "$selected" && -n "$default" ]]; then
    selected="$default"
  elif [[ "$selected" =~ ^[0-9]+$ ]] && (( selected >= 1 && selected <= ${#dataset_array[@]} )); then
    selected="${dataset_array[selected-1]}"
  fi
  
  while [[ -z "$selected" ]]; do
    echo "Invalid selection, please try again:"
    read -r selected
  done
  
  echo "$selected"
}
