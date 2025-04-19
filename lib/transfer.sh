#!/bin/bash
set -euo pipefail # Exit on error, unset var, pipe failure
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
  local transfer_cmd # Define the command string variable

  if [[ "$src_host" == "local" && "$dst_host" == "local" ]]; then
    # Local to local transfer
    transfer_cmd="$send_cmd | pv -petars $size | $recv_cmd \"${dst_dataset}\""
  elif [[ "$src_host" == "local" ]]; then
    # Local to remote transfer
    local remote_recv_cmd="$recv_cmd '${dst_dataset}'" # Quote dataset name for remote shell
    local pipe_prefix="$send_cmd"
    local pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$remote_recv_cmd\""

    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
      pipe_prefix="$pipe_prefix | $compress_cmd"
      pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$decompress_cmd | $remote_recv_cmd\""
    fi

    # Add mbuffer if applicable (only when not using ZFS native resume)
    if [[ "$RESUME_SUPPORT" == "true" && "$send_args" != -t* ]]; then
       log "Using mbuffer for non-native resume support."
       pipe_prefix="$pipe_prefix | mbuffer -q -m 1G"
       pipe_suffix="ssh \"$ssh_user@$dst_host\" \"mbuffer -q -m 1G | ${decompress_cmd:-cat} | $remote_recv_cmd\"" # Use cat if no decompress_cmd
    fi

    transfer_cmd="$pipe_prefix | pv -petars $size | $pipe_suffix"

  elif [[ "$dst_host" == "local" ]]; then
    # Remote to local transfer
    local remote_send_cmd="$send_cmd" # send_cmd already includes snapshot/token
    local pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_cmd\""
    local pipe_suffix="$recv_cmd \"${dst_dataset}\""

    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
      pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_cmd | $compress_cmd\""
      pipe_suffix="$decompress_cmd | $pipe_suffix"
    fi

    # Add mbuffer if applicable
    if [[ "$RESUME_SUPPORT" == "true" && "$send_args" != -t* ]]; then
       log "Using mbuffer for non-native resume support."
       pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_cmd | ${compress_cmd:-cat} | mbuffer -q -m 1G\""
       pipe_suffix="mbuffer -q -m 1G | ${decompress_cmd:-cat} | $pipe_suffix"
    fi

    transfer_cmd="$pipe_prefix | pv -petars $size | $pipe_suffix"

  else
    # Remote to remote transfer
    local remote_send_part="$send_cmd" # send_cmd already includes snapshot/token
    local remote_recv_part="$recv_cmd '${dst_dataset}'" # Quote dataset name for remote shell

    if [[ "$DIRECT_REMOTE_TRANSFER" == "true" ]]; then
      log "Performing direct remote-to-remote transfer (requires SSH keys between $src_host and $dst_host)."
      log "Note: Progress monitoring (pv) will reflect source send rate, not overall transfer."

      local direct_cmd_part1="ssh -o ConnectTimeout=\"$SSH_TIMEOUT\" \"$ssh_user@$src_host\" \"$remote_send_part"
      local direct_cmd_part2="ssh -o ConnectTimeout=\"$SSH_TIMEOUT\" \"$ssh_user@$dst_host\" \""

      if $USE_COMPRESSION; then
        if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
        direct_cmd_part1="$direct_cmd_part1 | $compress_cmd"
        direct_cmd_part2="$direct_cmd_part2 $decompress_cmd |"
      fi

      # Add pv on the source side before piping to the destination host
      direct_cmd_part1="$direct_cmd_part1 | pv -pterab\""
      # Complete the destination command
      direct_cmd_part2="$direct_cmd_part2 $remote_recv_part\""

      transfer_cmd="$direct_cmd_part1 | $direct_cmd_part2"

    else
      # Default: Proxy through local machine
      log "Performing remote-to-remote transfer via local proxy."
      local pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_part\""
      local pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$remote_recv_part\""

      if $USE_COMPRESSION; then
        if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
        pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_part | $compress_cmd\""
        pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$decompress_cmd | $remote_recv_part\""
      fi

      transfer_cmd="$pipe_prefix | pv -petars $size | $pipe_suffix"
    fi
  fi

  # Execute the constructed command using the helper
  execute_or_log_command "$transfer_cmd" "Initial transfer pipeline failed"
  # The helper function handles errors and logging

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
  local transfer_cmd # Define the command string variable
  local send_options="-i \"${src_dataset}@${src_prev_snapshot}\" \"${src_dataset}@${src_snapshot}\""

  if [[ "$src_host" == "local" && "$dst_host" == "local" ]]; then
    # Local to local transfer
    transfer_cmd="$send_cmd $send_options | pv -pterab | $recv_cmd \"${dst_dataset}\""
  elif [[ "$src_host" == "local" ]]; then
    # Local to remote transfer
    local remote_recv_cmd="$recv_cmd '${dst_dataset}'" # Quote dataset name for remote shell
    local pipe_prefix="$send_cmd $send_options"
    local pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$remote_recv_cmd\""

    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
      pipe_prefix="$pipe_prefix | $compress_cmd"
      pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$decompress_cmd | $remote_recv_cmd\""
    fi

    transfer_cmd="$pipe_prefix | pv -pterab | $pipe_suffix"

  elif [[ "$dst_host" == "local" ]]; then
    # Remote to local transfer
    # Ensure proper quoting for snapshot names in the remote command
    local remote_send_options="-i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}'"
    local remote_send_cmd="$send_cmd $remote_send_options"
    local pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_cmd\""
    local pipe_suffix="$recv_cmd \"${dst_dataset}\""

    if $USE_COMPRESSION; then
      if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
      pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_cmd | $compress_cmd\""
      pipe_suffix="$decompress_cmd | $pipe_suffix"
    fi

    transfer_cmd="$pipe_prefix | pv -pterab | $pipe_suffix"

  else
    # Remote to remote transfer
    # Ensure proper quoting for snapshot names in the remote command
    local remote_send_options="-i '${src_dataset}@${src_prev_snapshot}' '${src_dataset}@${src_snapshot}'"
    local remote_send_part="$send_cmd $remote_send_options"
    local remote_recv_part="$recv_cmd '${dst_dataset}'" # Quote dataset name for remote shell

    if [[ "$DIRECT_REMOTE_TRANSFER" == "true" ]]; then
      log "Performing direct remote-to-remote incremental transfer (requires SSH keys between $src_host and $dst_host)."
      log "Note: Progress monitoring (pv) will reflect source send rate, not overall transfer."

      local direct_cmd_part1="ssh -o ConnectTimeout=\"$SSH_TIMEOUT\" \"$ssh_user@$src_host\" \"$remote_send_part"
      local direct_cmd_part2="ssh -o ConnectTimeout=\"$SSH_TIMEOUT\" \"$ssh_user@$dst_host\" \""

      if $USE_COMPRESSION; then
        if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
        direct_cmd_part1="$direct_cmd_part1 | $compress_cmd"
        direct_cmd_part2="$direct_cmd_part2 $decompress_cmd |"
      fi

      # Add pv on the source side before piping to the destination host
      direct_cmd_part1="$direct_cmd_part1 | pv -pterab\""
      # Complete the destination command
      direct_cmd_part2="$direct_cmd_part2 $remote_recv_part\""

      transfer_cmd="$direct_cmd_part1 | $direct_cmd_part2"

    else
      # Default: Proxy through local machine
      log "Performing remote-to-remote incremental transfer via local proxy."
      local pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_part\""
      local pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$remote_recv_part\""

      if $USE_COMPRESSION; then
        if which pigz >/dev/null; then local compress_cmd="pigz"; local decompress_cmd="pigz -d"; else local compress_cmd="gzip"; local decompress_cmd="gzip -d"; fi
        pipe_prefix="ssh \"$ssh_user@$src_host\" \"$remote_send_part | $compress_cmd\""
        pipe_suffix="ssh \"$ssh_user@$dst_host\" \"$decompress_cmd | $remote_recv_part\""
      fi

      transfer_cmd="$pipe_prefix | pv -pterab | $pipe_suffix"
    fi
  fi

  # Execute the constructed command using the helper
  execute_or_log_command "$transfer_cmd" "Incremental transfer pipeline failed"
  # The helper function handles errors and logging

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
    local destroy_cmd
    if [[ "$src_host" == "local" ]]; then
      destroy_cmd="zfs destroy -r \"${src_dataset}@${sync_snapshot}\""
    else
      destroy_cmd="ssh \"$ssh_user@$src_host\" \"zfs destroy -r '${src_dataset}@${sync_snapshot}'\""
    fi
    # Use helper, but don't exit on error (just warn)
    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would execute: $destroy_cmd"
    else
        log "Executing: $destroy_cmd"
        eval "$destroy_cmd" 2>/dev/null || warn "Could not destroy old sync snapshot on source: ${src_dataset}@${sync_snapshot}"
    fi
  fi

  # Create or rename sync snapshot on source
  if [[ "$create_new" == "true" ]]; then
    log "Creating new sync snapshot on source"
    local r_flag=""
    [[ "$recursive" == "true" ]] && r_flag="-r"

    local snapshot_cmd
    if [[ "$src_host" == "local" ]]; then
      snapshot_cmd="zfs snapshot $r_flag \"${src_dataset}@${sync_snapshot}\""
    else
      snapshot_cmd="ssh \"$ssh_user@$src_host\" \"zfs snapshot $r_flag '${src_dataset}@${sync_snapshot}'\""
    fi
    execute_or_log_command "$snapshot_cmd" "Failed to create sync snapshot on source"
  else
    log "Renaming current snapshot to sync snapshot on source"
    local rename_cmd
    if [[ "$src_host" == "local" ]]; then
      rename_cmd="zfs rename \"${src_dataset}@${snapshot}\" \"${src_dataset}@${sync_snapshot}\""
    else
      rename_cmd="ssh \"$ssh_user@$src_host\" \"zfs rename '${src_dataset}@${snapshot}' '${src_dataset}@${sync_snapshot}'\""
    fi
    execute_or_log_command "$rename_cmd" "Failed to rename to sync snapshot on source"
  fi

  # Handle destination side sync snapshot
  if get_snapshot "$dst_host" "$dst_dataset" "$sync_snapshot" "$ssh_user"; then
    log "Removing existing sync snapshot on destination"
    local destroy_cmd
    if [[ "$dst_host" == "local" ]]; then
      destroy_cmd="zfs destroy -r \"${dst_dataset}@${sync_snapshot}\""
    else
      destroy_cmd="ssh \"$ssh_user@$dst_host\" \"zfs destroy -r '${dst_dataset}@${sync_snapshot}'\""
    fi
    # Use helper, but don't exit on error (just warn)
     if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would execute: $destroy_cmd"
    else
        log "Executing: $destroy_cmd"
        eval "$destroy_cmd" 2>/dev/null || warn "Could not destroy old sync snapshot on destination: ${dst_dataset}@${sync_snapshot}"
    fi
  fi

  # Create or rename sync snapshot on destination
  if [[ "$create_new" == "true" ]]; then
    log "Creating new sync snapshot on destination"
    local r_flag=""
    [[ "$recursive" == "true" ]] && r_flag="-r"

    local snapshot_cmd
    if [[ "$dst_host" == "local" ]]; then
      snapshot_cmd="zfs snapshot $r_flag \"${dst_dataset}@${sync_snapshot}\""
    else
      snapshot_cmd="ssh \"$ssh_user@$dst_host\" \"zfs snapshot $r_flag '${dst_dataset}@${sync_snapshot}'\""
    fi
    execute_or_log_command "$snapshot_cmd" "Failed to create sync snapshot on destination"
  else
    log "Renaming current snapshot to sync snapshot on destination"
    local rename_cmd
    if [[ "$dst_host" == "local" ]]; then
      rename_cmd="zfs rename \"${dst_dataset}@${snapshot}\" \"${dst_dataset}@${sync_snapshot}\""
    else
      rename_cmd="ssh \"$ssh_user@$dst_host\" \"zfs rename '${dst_dataset}@${snapshot}' '${dst_dataset}@${sync_snapshot}'\""
    fi
    execute_or_log_command "$rename_cmd" "Failed to rename to sync snapshot on destination"
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
