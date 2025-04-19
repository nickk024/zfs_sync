#!/bin/bash
# ZFS Sync - Utility Functions
# --------------------------------

# Check if we're running as root, and if not, re-run with sudo
check_sudo() {
  if [[ $EUID -ne 0 ]]; then
    log "This script requires root privileges. Requesting elevation..."
    # Pass all original arguments and environment variables to the sudo command
    exec sudo -E "$0" "$@"
    # If exec fails, script will continue and error out in the check_root function
  fi
}

# Separate function to check if we have root, used after sudo elevation
check_root() {
  if [[ $EUID -ne 0 ]]; then
    error "This script must be run as root. Please run with sudo."
  fi
  log "Running with root privileges."
}

# Check for required software and dependencies
check_prerequisites() {
  log "Checking for required tools..."
  
  # Check for essential commands
  for cmd in zfs ssh; do
    which $cmd >/dev/null || error "$cmd command not found. This is required."
  done
  
  # Check for pv (progress viewer)
  which pv >/dev/null || error "Required command 'pv' not found. Please install it (e.g., 'sudo apt install pv' or 'sudo yum install pv')."
  
  # Check for compression tools
  if $USE_COMPRESSION; then
    if which pigz >/dev/null; then
      log "Using pigz for compression (faster parallel gzip)"
    else
      log "pigz not found. Will use gzip instead (slower compression)"
      if ! which gzip >/dev/null; then
        error "Neither pigz nor gzip found. Cannot use compression."
      fi
    fi
  fi
  
  # Check for resumable transfer support
  if $RESUME_SUPPORT; then
    # Check for mbuffer only if resume support is explicitly enabled
    which mbuffer >/dev/null || error "Command 'mbuffer' not found, but RESUME_SUPPORT is enabled in the configuration. Please install mbuffer (e.g., 'sudo apt install mbuffer' or 'sudo yum install mbuffer') or disable RESUME_SUPPORT."
  fi
  
  log "Prerequisite checks completed."
}

# Verify SSH connection and ZFS availability
verify_ssh() {
  local host=$1
  local ssh_user=$2
  
  # Skip for local connections
  if [[ "$host" == "local" ]]; then
    return 0
  fi
  
  log "Verifying SSH connection to $ssh_user@$host..."
  
  # Test SSH connection and ZFS availability in a single command
  if ! ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" 'echo "SSH connection successful." >&2 && which zfs >/dev/null'; then
      # Determine the likely cause of failure (more complex to do perfectly without multiple calls, but we can make an educated guess)
      # Attempt a simple connection test again to differentiate connection vs zfs command failure
      if ! ssh -o BatchMode=yes -o ConnectTimeout=2 "$ssh_user@$host" 'exit 0'; then
          error "SSH connection failed to $ssh_user@$host. Please check connectivity and SSH key setup (e.g., ssh-copy-id $ssh_user@$host)."
      else
          # If connection works but the combined command failed, it's likely 'which zfs' failed
          error "SSH connection successful, but ZFS command not found or not executable on $host. Please ensure ZFS is installed and accessible by user $ssh_user."
      fi
  fi
  
  log "SSH connection and remote ZFS verified on $host."
}

# Prompt yes/no question with default
prompt_yes_no() {
  local prompt=$1
  local default=$2
  
  local yn_prompt
  if [[ "$default" == "y" ]]; then
    yn_prompt="[Y/n]"
  else
    yn_prompt="[y/N]"
  fi
  
  echo -n "$prompt $yn_prompt: "
  read answer
  
  if [[ -z "$answer" ]]; then
    answer="$default"
  fi
  
  if [[ "${answer,,}" == "y" || "${answer,,}" == "yes" ]]; then
    return 0
  else
    return 1
  fi
}

# Execute a command or log it if DRY_RUN is true
# Usage: execute_or_log_command "command string" [error_message_on_fail]
execute_or_log_command() {
  local cmd_string=$1
  local error_msg=${2:-"Command failed"} # Optional error message

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY RUN] Would execute: $cmd_string"
    return 0 # Assume success in dry run
  else
    log "Executing: $cmd_string"
    local output
    local result=0
    # Use eval carefully, assuming commands are constructed safely within the script
    output=$(eval "$cmd_string" 2>&1) || result=$?
    if [[ $result -ne 0 ]]; then
      error "$error_msg (Exit code: $result). Output: $output"
      # error function exits the script
    else
      # Log successful command output if any (useful for some commands)
      [[ -n "$output" ]] && log "Command output: $output"
      return 0
    fi
  fi
}

# Interactive setup function
run_interactive_setup() {
  log "Starting Interactive Setup..."

  # Load defaults from .env if available (config should have been loaded already)
  local default_src_host=${DEFAULT_SOURCE_HOST:-"local"}
  local default_dst_host=${DEFAULT_DEST_HOST:-""}
  local default_ssh_user=${DEFAULT_SSH_USER:-"root"}
  local default_src_dataset=${DEFAULT_SOURCE_DATASET:-""}
  local default_dst_dataset=${DEFAULT_DEST_DATASET:-""}
  local default_recursive=${DEFAULT_RECURSIVE:-"true"}
  local default_compression=${DEFAULT_USE_COMPRESSION:-"true"}
  local default_resume=${DEFAULT_RESUME_SUPPORT:-"true"}
  local default_snapshot_prefix=${DEFAULT_SNAPSHOT_PREFIX:-"backup"}
  local default_max_snapshots=${DEFAULT_MAX_SNAPSHOTS:-5}

  # --- Gather Parameters ---
  echo "--- Interactive Configuration ---"

  # Source Host
  echo -n "Enter Source Host [$default_src_host]: "
  read interactive_src_host
  interactive_src_host=${interactive_src_host:-$default_src_host}

  # Destination Host
  echo -n "Enter Destination Host [$default_dst_host]: "
  read interactive_dst_host
  interactive_dst_host=${interactive_dst_host:-$default_dst_host}
  if [[ -z "$interactive_dst_host" ]]; then error "Destination Host is required."; fi

  # SSH User
  echo -n "Enter SSH User [$default_ssh_user]: "
  read interactive_ssh_user
  interactive_ssh_user=${interactive_ssh_user:-$default_ssh_user}

  # Verify SSH connections before proceeding
  log "Verifying SSH connectivity..."
  verify_ssh "$interactive_src_host" "$interactive_ssh_user"
  verify_ssh "$interactive_dst_host" "$interactive_ssh_user"
  log "SSH connectivity verified."

  # Source Dataset (using select_dataset from datasets.sh)
  # Need to make sure select_dataset is sourced or available
  # Assuming datasets.sh is sourced by the main script
  interactive_src_dataset=$(select_dataset "$interactive_src_host" "Select Source Dataset" "$default_src_dataset" "$interactive_ssh_user")
  if [[ -z "$interactive_src_dataset" ]]; then error "Source Dataset is required."; fi

  # Destination Dataset
  local suggested_dst_dataset=${default_dst_dataset:-$interactive_src_dataset}
  echo -n "Enter Destination Dataset [$suggested_dst_dataset]: "
  read interactive_dst_dataset
  interactive_dst_dataset=${interactive_dst_dataset:-$suggested_dst_dataset}
  if [[ -z "$interactive_dst_dataset" ]]; then error "Destination Dataset is required."; fi

  # Other Options (using prompt_yes_no)
  prompt_yes_no "Recursive transfer?" "$default_recursive" && interactive_recursive="true" || interactive_recursive="false"
  prompt_yes_no "Use compression?" "$default_compression" && interactive_compression="true" || interactive_compression="false"
  prompt_yes_no "Enable resume support?" "$default_resume" && interactive_resume="true" || interactive_resume="false"

  # Snapshot Prefix
  echo -n "Enter Snapshot Prefix [$default_snapshot_prefix]: "
  read interactive_snapshot_prefix
  interactive_snapshot_prefix=${interactive_snapshot_prefix:-$default_snapshot_prefix}

  # Max Snapshots
  echo -n "Enter Max Snapshots to keep [$default_max_snapshots]: "
  read interactive_max_snapshots
  interactive_max_snapshots=${interactive_max_snapshots:-$default_max_snapshots}
  # Basic validation for number
  if ! [[ "$interactive_max_snapshots" =~ ^[0-9]+$ ]]; then
      error "Max Snapshots must be a number."
  fi

  echo "--- Configuration Summary ---"
  log "Interactive Configuration Summary:"
  log "  Source:           $interactive_ssh_user@$interactive_src_host:$interactive_src_dataset"
  log "  Destination:      $interactive_ssh_user@$interactive_dst_host:$interactive_dst_dataset"
  log "  Recursive:        $interactive_recursive"
  log "  Snapshot Prefix:  $interactive_snapshot_prefix"
  log "  Max Snapshots:    $interactive_max_snapshots"
  log "  Compression:      $interactive_compression"
  log "  Resume Support:   $interactive_resume"
  log "---"

  # Confirm before running
  if ! prompt_yes_no "Proceed with this configuration?" "y"; then
      log "User aborted interactive setup."
      exit 1
  fi

  # --- Execute the Job ---
  # We need to call a modified run_job or a new function that accepts these params
  # For now, let's just log that we would run it.
  # TODO: Refactor run_job or create run_job_interactive
  log "[INTERACTIVE MODE] Would now execute the sync job with the above parameters."
  log "Need to refactor run_job to accept parameters instead of job_name."

  # Placeholder exit code
  return 0
}
