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
  which pv >/dev/null || { 
    log "pv command not found. Installing..."
    if which apt-get >/dev/null; then
      sudo apt-get update -q && sudo apt-get install -y pv || error "Failed to install pv"
    elif which yum >/dev/null; then
      sudo yum install -y pv || error "Failed to install pv"
    else
      error "pv command not found and could not determine package manager. Please install pv manually."
    fi
  }
  
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
    which mbuffer >/dev/null || { 
      log "mbuffer command not found but resumable transfers enabled. Installing..."
      if which apt-get >/dev/null; then
        sudo apt-get update -q && sudo apt-get install -y mbuffer || {
          warn "Failed to install mbuffer automatically. Disabling resume support."
          RESUME_SUPPORT=false
        }
      elif which yum >/dev/null; then
        sudo yum install -y mbuffer || {
          warn "Failed to install mbuffer automatically. Disabling resume support."
          RESUME_SUPPORT=false
        }
      else
        warn "Could not determine package manager. Disabling resume support."
        RESUME_SUPPORT=false
      fi
    }
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
  
  # Test SSH connection with timeout
  ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "echo SSH connection successful" || {
    error "SSH connection failed to $ssh_user@$host. Please setup SSH keys: ssh-copy-id $ssh_user@$host"
  }
  
  # Test ZFS availability on remote host
  ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$ssh_user@$host" "which zfs >/dev/null" || {
    error "ZFS command not available on $host. Please ensure ZFS is installed."
  }
  
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
