#!/bin/bash
set -euo pipefail # Exit on error, unset var, pipe failure
# ZFS Intelligent Replication Script - Main (Multi-Job Support)
# --------------------------------

# ========== LOAD MODULES ==========

# Get script directory for relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Source modules - config MUST be sourced before others rely on its variables
source "$SCRIPT_DIR/lib/config.sh"
source "$SCRIPT_DIR/lib/logging.sh"
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/datasets.sh"
source "$SCRIPT_DIR/lib/transfer.sh"

# ========== ARGUMENT PARSING ==========

SELECTED_JOBS=()
RUN_ALL_JOBS=false
HELP_REQUESTED=false
DRY_RUN=false # Add dry-run flag

# Function to show help message
usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --job <job_name>     Run only the specified job (defined in .env). Can be used multiple times."
  echo "  --all-jobs           Run all valid jobs defined in ZFS_SYNC_JOB_NAMES in the .env file."
  echo "  --dry-run            Show what commands would be executed without actually running them."
  echo "  --help               Show this help message."
  echo ""
  echo "Configuration is managed via the .env file."
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --job)
      if [[ -z "${2:-}" ]]; then error "Missing job name for --job option."; fi
      SELECTED_JOBS+=("$2")
      shift 2
      ;;
    --all-jobs)
      RUN_ALL_JOBS=true
      shift
      ;;
    --help)
      usage # Show usage and exit
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

# ========== JOB EXECUTION FUNCTION ==========

run_job() {
    local job_name=$1
    log "================================================="
    log "Starting Job: $job_name"
    log "================================================="

    # --- Retrieve Job-Specific Configuration ---
    # Mandatory settings (already validated during load_config)
    local SOURCE_HOST="${JOB_SOURCE_HOST[$job_name]}"
    local SOURCE_DATASET="${JOB_SOURCE_DATASET[$job_name]}"
    local DEST_HOST="${JOB_DEST_HOST[$job_name]}"
    local DEST_DATASET="${JOB_DEST_DATASET[$job_name]}"

    # Settings with defaults
    local SSH_USER="${JOB_SSH_USER[$job_name]}"
    local SNAPSHOT_PREFIX="${JOB_SNAPSHOT_PREFIX[$job_name]}"
    local MAX_SNAPSHOTS="${JOB_MAX_SNAPSHOTS[$job_name]}"
    local RECURSIVE="${JOB_RECURSIVE[$job_name]}"
    local SYNC_SNAPSHOT="${JOB_SYNC_SNAPSHOT[$job_name]}" # Derived in config.sh

    # Export job-specific boolean/advanced flags AND global flags so library functions see them
    # This avoids needing to pass every single flag through every function signature for now.
    export USE_COMPRESSION="${JOB_USE_COMPRESSION[$job_name]}"
    export RESUME_SUPPORT="${JOB_RESUME_SUPPORT[$job_name]}"
    export DIRECT_REMOTE_TRANSFER="${JOB_DIRECT_REMOTE_TRANSFER[$job_name]}"
    export DRY_RUN # Export dry-run status
    # Note: DEBUG_MODE, SSH_TIMEOUT, CMD_TIMEOUT remain global

    log "Job Configuration:"
    log "  Source:           $SSH_USER@$SOURCE_HOST:$SOURCE_DATASET"
    log "  Destination:      $SSH_USER@$DEST_HOST:$DEST_DATASET"
    log "  Recursive:        $RECURSIVE"
    log "  Snapshot Prefix:  $SNAPSHOT_PREFIX"
    log "  Max Snapshots:    $MAX_SNAPSHOTS"
    log "  Sync Snapshot:    $SYNC_SNAPSHOT"
    log "  Compression:      $USE_COMPRESSION"
    log "  Resume Support:   $RESUME_SUPPORT"
    log "  Direct Remote:    $DIRECT_REMOTE_TRANSFER"
    log "---"

    # --- Job Logic (adapted from original main function) ---

    # Verify SSH connections for the specific job hosts
    log "Verifying SSH connectivity for job..."
    verify_ssh "$SOURCE_HOST" "$SSH_USER"
    verify_ssh "$DEST_HOST" "$SSH_USER"
    log "SSH connectivity verified."

    # Check if destination dataset exists and confirm source exists
    # Note: Multi-job mode implies non-interactive creation of destination if missing.
    echo ""
    log "Verifying datasets..."
    local CREATE_IF_MISSING="false"
    if ! has_dataset "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"; then
        log "Destination dataset $DEST_HOST:$DEST_DATASET doesn't exist. Will create during transfer."
        CREATE_IF_MISSING="true"
    else
        log "Destination dataset $DEST_HOST:$DEST_DATASET exists."
    fi

    # Confirm source exists and handle destination existence/creation flag
    # Pass job-specific RECURSIVE flag for potential cleanup operations inside confirm_dataset_exists
    local current_recursive_val=$RECURSIVE # Capture job-specific value
    export RECURSIVE # Export for confirm_dataset_exists to potentially use (though it reads the global)
                    # TODO: Refactor confirm_dataset_exists to accept recursive flag explicitly
    confirm_dataset_exists "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" "$SSH_USER" "$CREATE_IF_MISSING"
    local dest_exists_status=$? # Capture the exit code
    export RECURSIVE=$current_recursive_val # Restore potentially modified global (though unlikely needed here)
    debug "confirm_dataset_exists exit status: $dest_exists_status (0=exists/will_create, 1=does_not_exist)"


    # Determine if initial transfer is needed
    echo ""
    log "Checking snapshot status and determining transfer type..."
    local NEEDS_INITIAL_TRANSFER=false
    local SYNC_SNAPSHOT_SRC="" # Snapshot to use as base for incremental

    if [[ $dest_exists_status -eq 0 ]]; then # 0 means destination exists or will be created
        if has_dataset "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"; then
            log "Destination dataset exists. Checking for resume token or common snapshots."
            local resume_token_check="-"
            # Check for ZFS native resume token
            if [[ "$RESUME_SUPPORT" == "true" ]]; then
                log "Checking for ZFS receive resume token..." >&2
                if [[ "$DEST_HOST" == "local" ]]; then
                    resume_token_check=$(zfs get -H -o value receive_resume_token "$DEST_DATASET" 2>/dev/null || echo "-")
                else
                    resume_token_check=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$DEST_HOST" "zfs get -H -o value receive_resume_token '$DEST_DATASET'" 2>/dev/null || echo "-")
                fi
            fi

            if [[ "$resume_token_check" != "-" && -n "$resume_token_check" ]]; then
                log "Resume token found on destination. Attempting to resume full transfer." >&2
                NEEDS_INITIAL_TRANSFER=true
            else
                log "No resume token found. Checking for verified common snapshots (matching name and guid)..." >&2
                # Use enhanced function to find VERIFIED common snapshots
                COMMON_SNAPSHOTS=$(find_verified_common_snapshots "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" "$SSH_USER")

                if [[ -z "$COMMON_SNAPSHOTS" ]]; then
                    log "No verified common snapshots found between source and destination." >&2
                    # Check if source has *any* snapshots to decide if it's truly initial or just divergent
                    local source_has_snapshots=""
                    if [[ "$SOURCE_HOST" == "local" ]]; then
                        source_has_snapshots=$(zfs list -t snapshot -o name -H "$SOURCE_DATASET" | head -n 1)
                    else
                        source_has_snapshots=$(ssh -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$SOURCE_HOST" "zfs list -t snapshot -o name -H '$SOURCE_DATASET'" | head -n 1)
                    fi

                    if [[ -z "$source_has_snapshots" ]]; then
                        log "Source dataset $SOURCE_DATASET has no snapshots. Performing full initial transfer." >&2
                        NEEDS_INITIAL_TRANSFER=true
                    else
                        log "Source has snapshots, but none are common and verified with the destination. Performing full initial transfer." >&2
                        NEEDS_INITIAL_TRANSFER=true
                    fi
                else
                    SYNC_SNAPSHOT_SRC=$(echo "$COMMON_SNAPSHOTS" | head -1 | tr -d '\n\r')
                    log "Found verified common snapshot to use for incremental base: $SYNC_SNAPSHOT_SRC" >&2
                    NEEDS_INITIAL_TRANSFER=false
                fi
            fi
        else
            log "Destination dataset does not exist yet but will be created. Performing full initial transfer." >&2
            NEEDS_INITIAL_TRANSFER=true
        fi
    else
        # This case should not be reached if confirm_dataset_exists works correctly
        error "Reached unexpected state after dataset validation for job '$job_name'. Please check logs."
    fi

    # Create timestamp for new snapshot
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    NEW_SNAPSHOT="${SNAPSHOT_PREFIX}-${TIMESTAMP}"

    # Create source snapshot for transfer
    log "Creating new source snapshot for transfer: $NEW_SNAPSHOT"
    create_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$NEW_SNAPSHOT" "$RECURSIVE" "$SSH_USER"
    log "Created source snapshot: $SOURCE_DATASET@$NEW_SNAPSHOT"

    # Perform transfer
    echo ""
    echo "==== Starting ZFS Transfer for Job: $job_name ===="
    echo ""

    if [[ "$NEEDS_INITIAL_TRANSFER" == "true" ]]; then
        # Perform full transfer
        # Pass job-specific flags/timeouts to transfer function
        perform_full_transfer "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                             "$NEW_SNAPSHOT" "$SSH_USER" "$RECURSIVE"
                             # Note: transfer function uses exported vars for USE_COMPRESSION, RESUME_SUPPORT, DIRECT_REMOTE_TRANSFER, SSH_TIMEOUT

        # Create sync snapshot for future incremental transfers
        setup_sync_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                           "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT" "$SSH_USER" "$RECURSIVE" "true"
    else
        # Perform incremental transfer
        perform_incremental_transfer "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                                   "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT_SRC" "$SSH_USER" "$RECURSIVE"
                                   # Note: transfer function uses exported vars

        # Update sync snapshot for future incremental transfers
        setup_sync_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" \
                           "$NEW_SNAPSHOT" "$SYNC_SNAPSHOT" "$SSH_USER" "$RECURSIVE" "false"
    fi

    # Clean up old snapshots
    log "Cleaning up old snapshots for job '$job_name'..."
    clean_old_snapshots "$SOURCE_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$SOURCE_HOST" "$SSH_USER"
    clean_old_snapshots "$DEST_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$DEST_HOST" "$SSH_USER"

    # Print a summary for the job
    echo ""
    echo "==== ZFS Replication Summary for Job: $job_name ===="
    log "Job '$job_name' completed successfully at $(date)"

    # Show basic dataset stats after transfer
    echo ""
    echo "Source dataset information ($job_name):"
    show_dataset_info "$SOURCE_HOST" "$SOURCE_DATASET" "$SSH_USER"

    echo ""
    echo "Destination dataset information ($job_name):"
    show_dataset_info "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"

    log "================================================="
    log "Finished Job: $job_name"
    log "================================================="
    echo "" # Add space between job outputs
}

# ========== MAIN EXECUTION ==========

main() {
  # Load configuration from .env (parses globals and jobs into arrays)
  load_config

  log "Starting ZFS replication script (logging to $LOG_FILE)"

  # Check if we're running with sudo, and if not, re-run with sudo
  check_sudo "$@" # Pass original script args

  # Now we should be running as root
  check_root

  # Check prerequisites (globally)
  check_prerequisites
  debug "Prerequisites check completed"

  # Determine which jobs to run
  local jobs_to_run=()
  if $RUN_ALL_JOBS; then
      if [[ ${#ZFS_SYNC_JOBS[@]} -eq 0 ]]; then
          error "Option --all-jobs specified, but no valid jobs found in configuration (ZFS_SYNC_JOB_NAMES and related variables in .env)."
      fi
      log "Running all configured jobs: ${!ZFS_SYNC_JOBS[@]}"
      # Get keys (job names) from the ZFS_SYNC_JOBS associative array
      jobs_to_run=("${!ZFS_SYNC_JOBS[@]}")
  elif [[ ${#SELECTED_JOBS[@]} -gt 0 ]]; then
      log "Running selected jobs: ${SELECTED_JOBS[@]}"
      # Validate selected jobs against configured jobs
      local job_name
      for job_name in "${SELECTED_JOBS[@]}"; do
          if [[ -v ZFS_SYNC_JOBS[$job_name] ]]; then
              jobs_to_run+=("$job_name")
          else
              warn "Job '$job_name' specified via --job is not defined or is invalid in the .env file. Skipping."
          fi
      done
      if [[ ${#jobs_to_run[@]} -eq 0 ]]; then
          error "No valid jobs selected to run."
      fi
  else
      # Default behavior: No jobs specified, show help.
      log "No specific job requested and --all-jobs not specified."
      usage
  fi

  # Execute the selected jobs
  local job_name
  local overall_success=true
  for job_name in "${jobs_to_run[@]}"; do
      # Run the job logic in a subshell to isolate environment changes (like exported vars)
      ( run_job "$job_name" )
      local job_result=$?
      if [[ $job_result -ne 0 ]]; then
          log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
          log "ERROR: Job '$job_name' failed with exit code $job_result." >&2
          log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
          overall_success=false
          # Continue to next job unless we want to abort on first failure
          # error "Job '$job_name' failed. Aborting." # Uncomment to stop on first failure
      fi
  done

  log "================================================="
  if $overall_success; then
      log "All selected jobs completed successfully."
      echo "All selected jobs completed successfully."
  else
      log "One or more jobs failed. Please check the log file: $LOG_FILE" >&2
      echo "One or more jobs failed. Please check the log file: $LOG_FILE" >&2
      exit 1 # Exit with error code if any job failed
  fi
  log "Log file: $LOG_FILE"
  echo "Log file: $LOG_FILE"
}

# Run main function, passing along script arguments for sudo check
main "$@"
