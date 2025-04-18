#!/bin/bash
# ZFS Sync - Transfer Functions
# --------------------------------

# Performs a full ZFS transfer (initial replication)
perform_full_transfer() {
  local src_host=$1
  local src_dataset=$2
  local dst_host=$3
  local dst_dataset=$4
  local src_snapshot=$5
  local ssh_user=$6
  local recursive=$7
  
  log "Performing initial full transfer. This may take a long time..."
  
  # Calculate size for pv
  local size size_human
  if [[ "$src_host" == "local" ]]; then
    size=$(zfs list -o used -Hp "$src_dataset")
    size_human=$(zfs list -o used -H "$src_dataset")
  else
    size=$(ssh "$ssh_user@$src_host" "zfs list -o used -Hp '$src_dataset'")
    size_human=$(ssh "$ssh_user@$src_host" "zfs list -o used -H '$src_dataset'")
  fi
  log "Dataset size: $size_human"
  
  # Set up the replication command
  local send_cmd="zfs send"
  local recv_cmd="zfs receive"
  
  # Use -s on receive for full transfers to enable resume possibility on abort
  # Use -u to update (allows receiving into existing dataset if needed, harmless if dataset is new)
  # Do NOT use -F as it conflicts with -s and resumability
  recv_cmd="$recv_cmd -s -u -v" 
  
  # Check for resume token IF resume support is enabled
  local resume_token="-"
  local send_args=""
  if [[ "$RESUME_SUPPORT" == "true" ]]; then
    log "Checking for ZFS receive resume token on destination..." >&2
    if [[ "$dst_host" == "local" ]]; then
      resume_token=$(zfs get -H -o value receive_resume_token "$dst_dataset" 2>/dev/null || echo "-")
    else
      resume_token=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$dst_host" "zfs get -H -o value receive_resume_token '$dst_dataset'" 2>/dev/null || echo "-")
    fi
    
    if [[ "$resume_token" != "-" && -n "$resume_token" ]]; then
      log "Found resume token. Attempting to resume transfer." >&2
      # Use token for send command. -t replaces snapshot name and incremental flags.
      send_args="-t $resume_token" 
    else
      log "No resume token found. Starting new full transfer." >&2
      # Use standard full send arguments (snapshot name)
      send_args="${src_dataset}@${src_snapshot}"
    fi
  else
    log "ZFS native resume support disabled. Starting new full transfer." >&2
    # Use standard full send arguments (snapshot name)
    send_args="${src_dataset}@${src_snapshot}"
  fi

  # Construct the final send command
  # Add flags ONLY if NOT resuming with a token
  if [[ "$send_args" != -t* ]]; then
      # Add recursive flag if needed
      if [[ "$recursive" == "true" ]]; then
          send_cmd="$send_cmd -R"
      fi
      # Add verbose flag
      send_cmd="$send_cmd -v"
  fi
  # Append either the token or the snapshot name
  send_cmd="$send_cmd $send_args"

  # Perform the transfer
  log "Starting full transfer (resume token: $resume_token) at $(date)" >&2
  
  local result=0
  
  # Different command combinations based on source/destination and options
  if [[ "$src_host" == "local" && "$dst_host" == "local" ]]; then
    # Local to local transfer
    $send_cmd "${src_dataset}@${src_snapshot}" | pv -petars $size | $recv_cmd "${dst_dataset}"
    result=$?
  elif [[ "$src_host" == "local" ]]; then
    # Local to remote transfer
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      # NOTE: mbuffer usage might be slightly redundant if ZFS native resume is active,
      # but keeping it simplifies the logic vs dynamically removing it only when resuming.
      # The overhead should be minimal for large transfers.
      if $RESUME_SUPPORT; then
        # With compression and mbuffer
        $send_cmd | 
          $compress_cmd | 
          mbuffer -q -m 1G | 
          pv -petars $size | 
          ssh "$ssh_user@$dst_host" "mbuffer -q -m 1G | $decompress_cmd | $recv_cmd '${dst_dataset}'"
        result=$?
      else
        # With compression only (no mbuffer)
        $send_cmd | 
          $compress_cmd | 
          pv -petars $size | 
          ssh "$ssh_user@$dst_host" "$decompress_cmd | $recv_cmd '${dst_dataset}'"
        result=$?
      fi
    else # No compression
      if $RESUME_SUPPORT; then
        # With mbuffer only
        $send_cmd | 
          mbuffer -q -m 1G | 
          pv -petars $size | 
          ssh "$ssh_user@$dst_host" "mbuffer -q -m 1G | $recv_cmd '${dst_dataset}'"
        result=$?
      else
        # Basic transfer (no compression, no mbuffer)
        $send_cmd | 
          pv -petars $size | 
          ssh "$ssh_user@$dst_host" "$recv_cmd '${dst_dataset}'"
        result=$?
      fi
    fi
  elif [[ "$dst_host" == "local" ]]; then
    # Remote to local transfer
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      if $RESUME_SUPPORT; then
        # With compression and mbuffer
        ssh "$ssh_user@$src_host" "$send_cmd | $compress_cmd | mbuffer -q -m 1G" | 
          pv -petars $size | 
          mbuffer -q -m 1G | 
          $decompress_cmd | 
          $recv_cmd "${dst_dataset}"
        result=$?
      else
        # With compression only (no mbuffer)
        ssh "$ssh_user@$src_host" "$send_cmd | $compress_cmd" | 
          pv -petars $size | 
          $decompress_cmd | 
          $recv_cmd "${dst_dataset}"
        result=$?
      fi
    else # No compression
      if $RESUME_SUPPORT; then
        # With mbuffer only
        ssh "$ssh_user@$src_host" "$send_cmd | mbuffer -q -m 1G" | 
          pv -petars $size | 
          mbuffer -q -m 1G | 
          $recv_cmd "${dst_dataset}"
        result=$?
      else
        # Basic transfer (no compression, no mbuffer)
        ssh "$ssh_user@$src_host" "$send_cmd" | 
          pv -petars $size | 
          $recv_cmd "${dst_dataset}"
        result=$?
      fi
    fi
  else
    # Remote to remote transfer (proxy through local machine)
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      # Note: mbuffer less useful for remote-to-remote proxy
      ssh "$ssh_user@$src_host" "$send_cmd | $compress_cmd" | 
        pv -petars $size | 
        ssh "$ssh_user@$dst_host" "$decompress_cmd | $recv_cmd '${dst_dataset}'"
      result=$?
    else # No compression
      ssh "$ssh_user@$src_host" "$send_cmd" | 
        pv -petars $size | 
        ssh "$ssh_user@$dst_host" "$recv_cmd '${dst_dataset}'"
      result=$?
    fi
  fi
  
  [[ $result -ne 0 ]] && error "Initial transfer failed with code $result"
  
  log "Full transfer completed successfully"
  return $result
}

# Performs an incremental ZFS transfer
perform_incremental_transfer() {
  local src_host=$1
  local src_dataset=$2
  local dst_host=$3
  local dst_dataset=$4
  local src_snapshot=$5
  local src_prev_snapshot=$6  # Previous snapshot to use as base for incremental
  local ssh_user=$7
  local recursive=$8
  
  log "Performing incremental transfer from $src_host:$src_dataset"
  log "Using snapshots: $src_dataset@$src_prev_snapshot → $src_dataset@$src_snapshot"
  log "To destination: $dst_host:$dst_dataset"
  log "Transfer parameters: recursive=$recursive compression=$USE_COMPRESSION"
  
  log "Performing incremental transfer..."
  
  # Calculate size for pv (approximate for incremental)
  local size_estimate
  if [[ "$src_host" == "local" ]]; then
    size_estimate=$(zfs send -nvP -i "${src_dataset}@${src_prev_snapshot}" "${src_dataset}@${src_snapshot}" | grep "size" | awk '{print $2}')
  else
    size_estimate=$(ssh "$ssh_user@$src_host" "zfs send -nvP -i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}'" | grep "size" | awk '{print $2}')
  fi
  log "Estimated incremental size: $size_estimate bytes"
  
  # Set up the replication command
  local send_cmd="zfs send"
  local recv_cmd="zfs receive"
  
  # Add recursive flag if needed
  if [[ "$recursive" == "true" ]]; then
    send_cmd="$send_cmd -R"
  fi
  
  # Add verbose flag
  send_cmd="$send_cmd -v"
  # For incremental, don't force rollback (-F), just update (-u) and be verbose (-v)
  recv_cmd="$recv_cmd -uv" 
  
  # Perform the transfer
  log "Starting incremental transfer at $(date)"
  
  local result=0
  
  # Different command combinations based on source/destination and options
  if [[ "$src_host" == "local" && "$dst_host" == "local" ]]; then
    # Local to local transfer
    $send_cmd -i "${src_dataset}@${src_prev_snapshot}" "${src_dataset}@${src_snapshot}" | pv -pterab | $recv_cmd "${dst_dataset}"
    result=$?
  elif [[ "$src_host" == "local" ]]; then
    # Local to remote transfer
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      $send_cmd -i "${src_dataset}@${src_prev_snapshot}" "${src_dataset}@${src_snapshot}" | 
        $compress_cmd | 
        pv -pterab | 
        ssh "$ssh_user@$dst_host" "$decompress_cmd | $recv_cmd '${dst_dataset}'"
      result=$?
    else
      $send_cmd -i "${src_dataset}@${src_prev_snapshot}" "${src_dataset}@${src_snapshot}" | 
        pv -pterab | 
        ssh "$ssh_user@$dst_host" "$recv_cmd '${dst_dataset}'"
      result=$?
    fi
  elif [[ "$dst_host" == "local" ]]; then
    # Remote to local transfer
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      ssh "$ssh_user@$src_host" "$send_cmd -i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}' | $compress_cmd" | 
        pv -pterab | 
        $decompress_cmd | 
        $recv_cmd "${dst_dataset}"
      result=$?
    else
      ssh "$ssh_user@$src_host" "$send_cmd -i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}'" | 
        pv -pterab | 
        $recv_cmd "${dst_dataset}"
      result=$?
    fi
  else
    # Remote to remote transfer (proxy through local machine)
    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then
        local compress_cmd="pigz"
        local decompress_cmd="pigz -d"
      else
        local compress_cmd="gzip"
        local decompress_cmd="gzip -d"
      fi
      
      ssh "$ssh_user@$src_host" "$send_cmd -i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}' | $compress_cmd" | 
        pv -pterab | 
        ssh "$ssh_user@$dst_host" "$decompress_cmd | $recv_cmd '${dst_dataset}'"
      result=$?
    else
      ssh "$ssh_user@$src_host" "$send_cmd -i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}'" | 
        pv -pterab | 
        ssh "$ssh_user@$dst_host" "$recv_cmd '${dst_dataset}'"
      result=$?
    fi
  fi
  
  [[ $result -ne 0 ]] && error "Incremental transfer failed with code $result"
  
  log "Incremental transfer completed successfully"
  return $result
}

# Setup or update the sync snapshot for future transfers
setup_sync_snapshot() {
  local src_host=$1
  local src_dataset=$2
  local dst_host=$3
  local dst_dataset=$4
  local snapshot=$5      # Current snapshot name
  local sync_snapshot=$6 # Name to use for sync snapshot
  local ssh_user=$7
  local recursive=$8
  local create_new=$9   # Whether to create new snapshots or rename existing ones
  
  # Verify both datasets exist
  if ! has_dataset "$src_host" "$src_dataset" "$ssh_user"; then
    error "Source dataset $src_dataset no longer exists on $src_host!"
  fi
  
  if ! has_dataset "$dst_host" "$dst_dataset" "$ssh_user"; then
    error "Destination dataset $dst_dataset doesn't exist on $dst_host!"
  fi
  
  log "Setting up common sync snapshot on both hosts"
  
  # Handle source side sync snapshot
  if get_snapshot "$src_host" "$src_dataset" "$sync_snapshot" "$ssh_user"; then
    log "Removing existing sync snapshot on source"
    if [[ "$src_host" == "local" ]]; then
      zfs destroy -r "${src_dataset}@${sync_snapshot}" 2>/dev/null || warn "Could not destroy old sync snapshot on source"
    else
      ssh "$ssh_user@$src_host" "zfs destroy -r '${src_dataset}@${sync_snapshot}'" 2>/dev/null || warn "Could not destroy old sync snapshot on source"
    fi
  fi
  
  # Create or rename sync snapshot on source
  if [[ "$create_new" == "true" ]]; then
    log "Creating new sync snapshot on source"
    local r_flag=""
    [[ "$recursive" == "true" ]] && r_flag="-r"
    
    if [[ "$src_host" == "local" ]]; then
      zfs snapshot $r_flag "${src_dataset}@${sync_snapshot}" || error "Failed to create sync snapshot on source"
    else
      ssh "$ssh_user@$src_host" "zfs snapshot $r_flag '${src_dataset}@${sync_snapshot}'" || error "Failed to create sync snapshot on source"
    fi
  else
    log "Renaming current snapshot to sync snapshot on source"
    if [[ "$src_host" == "local" ]]; then
      zfs rename "${src_dataset}@${snapshot}" "${src_dataset}@${sync_snapshot}" || error "Failed to rename to sync snapshot on source"
    else
      ssh "$ssh_user@$src_host" "zfs rename '${src_dataset}@${snapshot}' '${src_dataset}@${sync_snapshot}'" || error "Failed to rename to sync snapshot on source"
    fi
  fi
  
  # Handle destination side sync snapshot
  if get_snapshot "$dst_host" "$dst_dataset" "$sync_snapshot" "$ssh_user"; then
    log "Removing existing sync snapshot on destination"
    if [[ "$dst_host" == "local" ]]; then
      zfs destroy -r "${dst_dataset}@${sync_snapshot}" 2>/dev/null || warn "Could not destroy old sync snapshot on destination"
    else
      ssh "$ssh_user@$dst_host" "zfs destroy -r '${dst_dataset}@${sync_snapshot}'" 2>/dev/null || warn "Could not destroy old sync snapshot on destination"
    fi
  fi
  
  # Create or rename sync snapshot on destination
  if [[ "$create_new" == "true" ]]; then
    log "Creating new sync snapshot on destination"
    local r_flag=""
    [[ "$recursive" == "true" ]] && r_flag="-r"
    
    if [[ "$dst_host" == "local" ]]; then
      zfs snapshot $r_flag "${dst_dataset}@${sync_snapshot}" || error "Failed to create sync snapshot on destination"
    else
      ssh "$ssh_user@$dst_host" "zfs snapshot $r_flag '${dst_dataset}@${sync_snapshot}'" || error "Failed to create sync snapshot on destination"
    fi
  else
    log "Renaming current snapshot to sync snapshot on destination"
    if [[ "$dst_host" == "local" ]]; then
      zfs rename "${dst_dataset}@${snapshot}" "${dst_dataset}@${sync_snapshot}" || error "Failed to rename to sync snapshot on destination"
    else
      ssh "$ssh_user@$dst_host" "zfs rename '${dst_dataset}@${snapshot}' '${dst_dataset}@${sync_snapshot}'" || error "Failed to rename to sync snapshot on destination"
    fi
  fi
  
  # Verify sync snapshots exist on both sides
  if ! get_snapshot "$src_host" "$src_dataset" "$sync_snapshot" "$ssh_user"; then
    error "Failed to verify sync snapshot on source after setup!"
  fi
  
  if ! get_snapshot "$dst_host" "$dst_dataset" "$sync_snapshot" "$ssh_user"; then
    error "Failed to verify sync snapshot on destination after setup!"
  fi
  
  log "Sync snapshots successfully set up on both source and destination"
}

# Display dataset information
show_dataset_info() {
  local host=$1
  local dataset=$2
  local ssh_user=$3
  
  if [[ "$host" == "local" ]]; then
    zfs list -o name,used,avail,refer "$dataset"
  else
    ssh "$ssh_user@$host" "zfs list -o name,used,avail,refer '$dataset'"
  fi
}
