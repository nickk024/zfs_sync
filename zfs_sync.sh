#!/bin/bash
# ZFS Intelligent Replication Script - Main
# --------------------------------
# This script handles efficient incremental replication between ZFS datasets
# Features:
# - Interactive selection of source and destination datasets
# - Automatically detects if initial full replication is needed
# - Creates and manages snapshots on both sides
# - Uses incremental sends for efficiency
# - Maintains synchronized snapshots for reliable transfers
# - Handles recursive datasets
# - Shows progress and estimated time remaining
# - Cleans up old snapshots according to retention policy

# ========== LOAD MODULES ==========

# Get script directory for relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Process command line arguments
NON_INTERACTIVE=false
HELP_REQUESTED=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --non-interactive)
      NON_INTERACTIVE=true
      shift
      ;;
    --help)
      HELP_REQUESTED=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Show help if requested
if [[ "$HELP_REQUESTED" == "true" ]]; then
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --non-interactive    Run with values from .env file without prompting"
  echo "  --help               Show this help message"
  exit 0
fi

# Source modules
source "$SCRIPT_DIR/lib/config.sh"
source "$SCRIPT_DIR/lib/logging.sh"
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/datasets.sh"
source "$SCRIPT_DIR/lib/transfer.sh"

# ========== MAIN EXECUTION ==========

main() {
  # Load configuration
  load_config
  
  log "Starting ZFS replication script (logging to $LOG_FILE)"
  
  # Check if we're running with sudo, and if not, re-run with sudo
  check_sudo "$@"
  
  # Now we should be running as root
  check_root
  
  # Check prerequisites
  check_prerequisites
  debug "Prerequisites check completed"
  
  echo ""
  echo "==== ZFS Replication Configuration ===="
  echo ""
  
  # Configuration can be interactive or from .env file
  if [[ "$NON_INTERACTIVE" == "false" ]]; then
    log "Beginning interactive configuration..."
    
    # Source configuration
    SOURCE_HOST="$DEFAULT_SOURCE_HOST"
    SSH_USER="$DEFAULT_SSH_USER"
    
    log "Using source host: $SOURCE_HOST"
    log "Using SSH user: $SSH_USER"
    
    # Verify SSH connection if source is remote
    verify_ssh "$SOURCE_HOST" "$SSH_USER"
    
    # Get and display source dataset choices
    log "Retrieving list of available source datasets..."
    SOURCE_DATASET=$(select_dataset "$SOURCE_HOST" "Select source dataset:" "" "$SSH_USER")
    if [[ -z "$SOURCE_DATASET" ]]; then
      error "No source dataset selected."
    fi
    
    log "Selected source dataset: $SOURCE_DATASET"
    
    # Verify source dataset exists
    log "Verifying source dataset..."
    if ! has_dataset "$SOURCE_HOST" "$SOURCE_DATASET" "$SSH_USER"; then
      error "Source dataset $SOURCE_DATASET does not exist on $SOURCE_HOST."
    fi
    
    # Destination dataset configuration
    echo ""
    log "Configuring destination host..."
    if [[ -z "$DEFAULT_DEST_HOST" ]]; then
      echo -n "Enter destination host IP/hostname (or 'local' for local transfers): "
      read DEST_HOST
    else
      echo -n "Enter destination host IP/hostname [default: $DEFAULT_DEST_HOST]: "
      read input
      DEST_HOST=${input:-$DEFAULT_DEST_HOST}
    fi
    
    log "Using destination host: $DEST_HOST"
    
    # Verify SSH connection if destination is remote
    verify_ssh "$DEST_HOST" "$SSH_USER"
    
    # Use same dataset name on destination by default
    echo ""
    log "Configuring destination dataset..."
    
    # Ask if user wants to use the same dataset name
    if prompt_yes_no "Use the same dataset name on destination ($SOURCE_DATASET)?" "y"; then
      DEST_DATASET="$SOURCE_DATASET"
      log "Using same dataset name on destination: $DEST_DATASET"
    else
      # Default destination dataset (derive from source dataset by default)
      DEFAULT_DEST_DATASET=$(echo "$SOURCE_DATASET" | sed 's|.*/||')
      DEFAULT_DEST_DATASET="tank/$DEFAULT_DEST_DATASET"
      
      log "Retrieving list of available destination datasets..."
      echo ""
      DEST_DATASET=$(select_dataset "$DEST_HOST" "Select destination dataset:" "$DEFAULT_DEST_DATASET" "$SSH_USER")
      if [[ -z "$DEST_DATASET" ]]; then
        error "No destination dataset selected."
      fi
      log "Selected destination dataset: $DEST_DATASET"
    fi
    
    # Recursive option
    echo ""
    log "Configuring recursive option..."
    if prompt_yes_no "Include child datasets (recursive)?" "y"; then
      RECURSIVE=true
      log "Recursive mode enabled"
    else
      RECURSIVE=false
      log "Recursive mode disabled"
    fi
  else
    # Non-interactive mode - use values from .env file
    log "Running in non-interactive mode with values from .env file"
    
    # Source host and SSH user
    SOURCE_HOST="$DEFAULT_SOURCE_HOST"
    SSH_USER="$DEFAULT_SSH_USER"
    log "Using source host: $SOURCE_HOST"
    log "Using SSH user: $SSH_USER"
    
    # Verify SSH connection if needed
    verify_ssh "$SOURCE_HOST" "$SSH_USER"
    
    # Use datasets from .env file
    SOURCE_DATASET="$DEFAULT_SOURCE_DATASET"
    if [[ -z "$SOURCE_DATASET" ]]; then
      error "No source dataset specified in .env file (DEFAULT_SOURCE_DATASET)"
    fi
    log "Using source dataset from config: $SOURCE_DATASET"
    
    # Verify source dataset exists
    log "Verifying source dataset..."
    if ! has_dataset "$SOURCE_HOST" "$SOURCE_DATASET" "$SSH_USER"; then
      error "Source dataset $SOURCE_DATASET does not exist on $SOURCE_HOST."
    fi
    
    # Destination configuration
    DEST_HOST="$DEFAULT_DEST_HOST"
    if [[ -z "$DEST_HOST" ]]; then
      error "No destination host specified in .env file (DEFAULT_DEST_HOST)"
    fi
    log "Using destination host from config: $DEST_HOST"
    
    # Verify SSH connection if needed
    verify_ssh "$DEST_HOST" "$SSH_USER"
    
    # Use destination dataset from .env
    DEST_DATASET="$DEFAULT_DEST_DATASET"
    if [[ -z "$DEST_DATASET" ]]; then
      # If not specified, use same as source
      DEST_DATASET="$SOURCE_DATASET"
    fi
    log "Using destination dataset from config: $DEST_DATASET"
    
    # Use recursive option from .env
    RECURSIVE="$DEFAULT_RECURSIVE"
    log "Using recursive option from config: $RECURSIVE"
  fi
  
  # Check if both datasets exist and handle creating new datasets if needed
  echo ""
  log "Verifying destination dataset..."
  CREATE_IF_MISSING="false"
  if ! has_dataset "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"; then
    log "Destination dataset doesn't exist."
    # In non-interactive mode, automatically agree to create it. Otherwise, prompt.
    if [[ "$NON_INTERACTIVE" == "true" ]] || prompt_yes_no "Create new dataset on destination?" "y"; then
      CREATE_IF_MISSING="true"
      log "Will create destination dataset during transfer"
    else
      error "Destination dataset doesn't exist and you chose not to create it. Aborting."
    fi
  else
    log "Destination dataset exists"
  fi
  
  # Confirm datasets exist on both sides (or will be created)
  echo ""
  log "Performing final dataset validation..."
  confirm_dataset_exists "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" "$SSH_USER" "$CREATE_IF_MISSING"
  local dest_exists_status=$? # Capture the exit code
  debug "confirm_dataset_exists exit status: $dest_exists_status (0=exists/will_create, 1=does_not_exist)"

  # Check for existing snapshots and create if needed
  echo ""
  log "Checking snapshot status..."

  # Check the exit status from confirm_dataset_exists
  if [[ $dest_exists_status -eq 0 ]]; then # 0 means destination exists or will be created
    # Destination exists or will be created. Now check if it *actually* exists *before* deciding transfer type.
    if has_dataset "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"; then
      log "Destination dataset exists."
      local resume_token_check="-"
      # Prioritize checking for a resume token if resume is enabled
      if [[ "$RESUME_SUPPORT" == "true" ]]; then
          log "Checking for ZFS receive resume token..." >&2
          if [[ "$DEST_HOST" == "local" ]]; then
              resume_token_check=$(zfs get -H -o value receive_resume_token "$DEST_DATASET" 2>/dev/null || echo "-")
          else
              resume_token_check=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$DEST_HOST" "zfs get -H -o value receive_resume_token '$DEST_DATASET'" 2>/dev/null || echo "-")
          fi
      fi

      if [[ "$resume_token_check" != "-" && -n "$resume_token_check" ]]; then
          # Resume token found! Force a full transfer, perform_full_transfer will handle the -t flag.
          log "Resume token found on destination. Attempting to resume full transfer." >&2
          NEEDS_INITIAL_TRANSFER=true
          # SYNC_SNAPSHOT_SRC is not needed when resuming with -t
      else
          # No resume token found, check for common snapshots for incremental.
          log "No resume token found. Checking for common snapshots..." >&2
          COMMON_SNAPSHOTS=$(find_common_snapshots "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" "$SSH_USER")

          if [[ -z "$COMMON_SNAPSHOTS" ]]; then
              # Destination exists, no resume token, no common snapshots. User wants this to fail during incremental.
              log "No common snapshots found. Attempting incremental transfer based on latest source snapshot." >&2
              LATEST_SOURCE_SNAPSHOT=$(zfs list -t snapshot -o name -S creation -H "$SOURCE_DATASET" | head -n 1 | awk -F@ '{print $2}')
              if [[ -z "$LATEST_SOURCE_SNAPSHOT" ]]; then
                  error "No snapshots found on source dataset $SOURCE_DATASET to attempt incremental transfer."
              fi
              log "WARNING: No common snapshot found. Will attempt incremental from latest source snapshot '$LATEST_SOURCE_SNAPSHOT', which will likely fail if destination is incompatible." >&2
              SYNC_SNAPSHOT_SRC="$LATEST_SOURCE_SNAPSHOT"
              NEEDS_INITIAL_TRANSFER=false # Force incremental attempt
          else
              # Destination exists, no resume token, common snapshots found. Proceed with incremental.
              SYNC_SNAPSHOT_SRC=$(echo "$COMMON_SNAPSHOTS" | head -1 | tr -d '\n\r')
              log "Found common snapshot to use: $SYNC_SNAPSHOT_SRC" >&2
              log "Will perform incremental transfer using this snapshot" >&2
              NEEDS_INITIAL_TRANSFER=false
          fi
      fi
    else
      # Destination does NOT exist yet, but will be created. Must do full transfer.
      log "Destination dataset does not exist yet but will be created. Performing full initial transfer." >&2
      NEEDS_INITIAL_TRANSFER=true
    fi
  else
    # This case means confirm_dataset_exists failed (e.g., dest doesn't exist and create=false)
    # The error function within confirm_dataset_exists should have already exited.
    # If we somehow reach here, it's an unexpected state.
    error "Reached unexpected state after dataset validation. Please check logs."
  fi
  
  # Create timestamp to be used in snapshot names (same for source and dest for better tracking)
  TIMESTAMP=$(date +%Y%m%d-%H%M%S)
  
  # Create source snapshot for transfer first
  NEW_SNAPSHOT="${SNAPSHOT_PREFIX}-${TIMESTAMP}"
  log "Creating new source snapshot for transfer: $NEW_SNAPSHOT"
  create_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$NEW_SNAPSHOT" "$RECURSIVE" "$SSH_USER"
  log "Created source snapshot: $SOURCE_DATASET@$NEW_SNAPSHOT"
  
  # Perform transfer
  echo ""
  echo "==== Starting ZFS Transfer ===="
  echo ""
  
  if [[ "$NEEDS_INITIAL_TRANSFER" == "true" ]]; then
    # Perform full transfer
    perform_full_transfer "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                         "$NEW_SNAPSHOT" "$SSH_USER" "$RECURSIVE"
    
    # Create sync snapshot for future incremental transfers
    setup_sync_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                       "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT" "$SSH_USER" "$RECURSIVE" "true"
  else
    # Perform incremental transfer
    perform_incremental_transfer "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                               "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT_SRC" "$SSH_USER" "$RECURSIVE"
    
    # Update sync snapshot for future incremental transfers
    setup_sync_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                       "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT" "$SSH_USER" "$RECURSIVE" "false"
  fi
  
  # Clean up old snapshots
  clean_old_snapshots "$SOURCE_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$SOURCE_HOST" "$SSH_USER"
  clean_old_snapshots "$DEST_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$DEST_HOST" "$SSH_USER"
  
  # Print a summary of what was done
  echo ""
  echo "==== ZFS Replication Summary ===="
  log "ZFS replication completed successfully at $(date)"
  
  # Show basic dataset stats after transfer
  echo ""
  echo "Source dataset information:"
  show_dataset_info "$SOURCE_HOST" "$SOURCE_DATASET" "$SSH_USER"
  
  echo ""
  echo "Destination dataset information:"
  show_dataset_info "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"
  
  # Print log file location for reference
  echo ""
  echo "Log file: $LOG_FILE"
}

# Run main function
main "$@"
