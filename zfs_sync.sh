#!/bin/bash

# ZFS Intelligent Replication Script
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

# ========== LOAD ENVIRONMENT VARIABLES ==========

# Load environment variables from .env file if it exists
if [[ -f "$(dirname "$0")/.env" ]]; then
    source "$(dirname "$0")/.env"
fi

# ========== CONFIGURATION ==========

# Default settings - can be overridden interactively or via .env file
DEFAULT_SOURCE_HOST=${DEFAULT_SOURCE_HOST:-"local"}
DEFAULT_DEST_HOST=${DEFAULT_DEST_HOST:-""}
DEFAULT_SSH_USER=${DEFAULT_SSH_USER:-"root"}
SNAPSHOT_PREFIX=${SNAPSHOT_PREFIX:-"backup"}
SYNC_SNAPSHOT="${SNAPSHOT_PREFIX}-sync"
MAX_SNAPSHOTS=${MAX_SNAPSHOTS:-5}  # How many snapshots to keep on source and destination

# Advanced options
USE_COMPRESSION=${USE_COMPRESSION:-true}    # Whether to use compression in transit
VERIFY_TRANSFERS=${VERIFY_TRANSFERS:-false}  # Whether to verify transfers (slows things down but adds security)
RESUME_SUPPORT=${RESUME_SUPPORT:-true}     # Whether to enable resumable transfers (needs mbuffer)

# ========== HELPER FUNCTIONS ==========

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $*" >&2
    exit 1
}

check_prerequisites() {
    which pv >/dev/null || { log "pv command not found. Installing..."; sudo apt-get install -y pv || error "Failed to install pv"; }
    
    if $USE_COMPRESSION; then
        which pigz >/dev/null || log "pigz not found. Will use gzip instead (slower compression)"
    fi
    
    if $RESUME_SUPPORT; then
        which mbuffer >/dev/null || { 
            log "mbuffer command not found but resumable transfers enabled. Installing..."
            sudo apt-get install -y mbuffer || {
                log "Failed to install mbuffer automatically. Disabling resume support."
                RESUME_SUPPORT=false
            }
        }
    fi
}

# Function to check if we're running as root, and if not, re-run with sudo
check_sudo() {
    if [[ $EUID -ne 0 ]]; then
        log "This script requires root privileges. Requesting elevation..."
        exec sudo "$0" "$@"
        # If exec fails, script will continue and error out in the check_root function
    fi
}

# Separate function to check if we have root, used after sudo elevation
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root. Please run with sudo."
    fi
}

verify_ssh() {
    local host=$1
    local ssh_user=$2
    log "Verifying SSH connection to $ssh_user@$host..."
    ssh -o BatchMode=yes "$ssh_user@$host" "echo SSH connection successful" || {
        error "SSH connection failed. Please setup SSH keys: ssh-copy-id $ssh_user@$host"
    }
    log "SSH connection verified."
}

get_datasets() {
    local host=$1
    local ssh_user=$2
    
    if [[ "$host" == "local" ]]; then
        zfs list -H -o name | grep -v "@" | sort
    else
        ssh "$ssh_user@$host" "zfs list -H -o name | grep -v '@' | sort"
    fi
}

has_dataset() {
    local host=$1
    local dataset=$2
    local ssh_user=$3
    
    if [[ "$host" == "local" ]]; then
        zfs list "$dataset" >/dev/null 2>&1
        return $?
    else
        ssh "$ssh_user@$host" "zfs list '$dataset'" >/dev/null 2>&1
        return $?
    fi
}

get_snapshot() {
    local host=$1
    local dataset=$2
    local snapshot=$3
    local ssh_user=$4
    
    if [[ "$host" == "local" ]]; then
        zfs list -t snapshot -o name "$dataset@$snapshot" >/dev/null 2>&1
        return $?
    else
        ssh "$ssh_user@$host" "zfs list -t snapshot -o name '$dataset@$snapshot'" >/dev/null 2>&1
        return $?
    fi
}

find_common_snapshots() {
    local src_host=$1
    local src_dataset=$2
    local dst_host=$3
    local dst_dataset=$4
    local ssh_user=$5
    
    log "Checking for common snapshots between source and destination..."
    
    # Get source snapshots
    if [[ "$src_host" == "local" ]]; then
        src_snaps=$(zfs list -t snapshot -o name -H "$src_dataset" 2>/dev/null | cut -d@ -f2 || echo "")
    else
        src_snaps=$(ssh "$ssh_user@$src_host" "zfs list -t snapshot -o name -H '$src_dataset'" 2>/dev/null | cut -d@ -f2 || echo "")
    fi
    
    # Get destination snapshots
    if [[ "$dst_host" == "local" ]]; then
        dst_snaps=$(zfs list -t snapshot -o name -H "$dst_dataset" 2>/dev/null | cut -d@ -f2 || echo "")
    else
        dst_snaps=$(ssh "$ssh_user@$dst_host" "zfs list -t snapshot -o name -H '$dst_dataset'" 2>/dev/null | cut -d@ -f2 || echo "")
    fi
    
    # Find common snapshots - make sure they are properly trimmed to avoid invalid names
    for snap in $src_snaps; do
        # Trim the snapshot name to remove any potential whitespace or newlines
        snap=$(echo "$snap" | tr -d '\n\r')
        
        # Check if this snapshot exists in destination snapshots
        echo "$dst_snaps" | grep -q "^$snap\$" && echo "$snap"
    done
}

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
    
    log "Creating snapshot $dataset@$snapshot on $host"
    if [[ "$host" == "local" ]]; then
        zfs snapshot $r_flag "$dataset@$snapshot" || error "Failed to create snapshot $dataset@$snapshot"
    else
        ssh "$ssh_user@$host" "zfs snapshot $r_flag '$dataset@$snapshot'" || error "Failed to create snapshot $dataset@$snapshot on $host"
    fi
}

clean_old_snapshots() {
    local dataset=$1
    local prefix=$2
    local keep=$3
    local host=$4
    local ssh_user=$5
    
    log "Cleaning old snapshots with prefix $prefix on $host..."
    
    local cmd="zfs list -t snapshot -o name -H | grep \"$dataset@$prefix\" | sort | head -n -$keep | xargs -r zfs destroy -v"
    
    if [[ "$host" == "local" ]]; then
        eval $cmd
    else
        ssh "$ssh_user@$host" "$cmd"
    fi
}

select_dataset() {
    local host=$1
    local prompt=$2
    local default=$3
    local ssh_user=$4
    
    echo "$prompt"
    
    if [[ "$host" == "local" ]]; then
        echo "Available datasets on local:"
    else
        echo "Available datasets on $host:"
    fi
    
    local datasets=$(get_datasets "$host" "$ssh_user")
    local i=1
    local dataset_array=()
    
    while read -r dataset; do
        echo "  $i) $dataset"
        dataset_array+=("$dataset")
        ((i++))
    done <<< "$datasets"
    
    local selected
    echo -n "Enter dataset number or full dataset name [default: $default]: "
    read selected
    
    if [[ -z "$selected" ]]; then
        selected="$default"
    elif [[ "$selected" =~ ^[0-9]+$ ]] && (( selected >= 1 && selected < i )); then
        selected="${dataset_array[selected-1]}"
    fi
    
    echo "$selected"
}

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

# ========== MAIN EXECUTION ==========

main() {
    log "Starting ZFS replication script"
    
    # Check if we're running with sudo, and if not, re-run with sudo
    check_sudo "$@"
    
    # Now we should be running as root
    check_root
    check_prerequisites
    
    # Gather parameters interactively
    echo "==== ZFS Replication Configuration ===="
    
    # Source configuration
    SOURCE_HOST="$DEFAULT_SOURCE_HOST"
    SSH_USER="$DEFAULT_SSH_USER"
    
    if [[ "$SOURCE_HOST" != "local" ]]; then
        verify_ssh "$SOURCE_HOST" "$SSH_USER"
    fi
    
    SOURCE_DATASET=$(select_dataset "$SOURCE_HOST" "Select source dataset:" "" "$SSH_USER")
    if [[ -z "$SOURCE_DATASET" ]]; then
        error "No source dataset selected."
    fi
    
    # Verify source dataset exists
    if ! has_dataset "$SOURCE_HOST" "$SOURCE_DATASET" "$SSH_USER"; then
        error "Source dataset $SOURCE_DATASET does not exist on $SOURCE_HOST."
    fi
    
    # Destination configuration
    if [[ -z "$DEFAULT_DEST_HOST" ]]; then
        echo -n "Enter destination host IP/hostname (or 'local' for local transfers): "
        read DEST_HOST
    else
        echo -n "Enter destination host IP/hostname [default: $DEFAULT_DEST_HOST]: "
        read input
        DEST_HOST=${input:-$DEFAULT_DEST_HOST}
    fi
    
    if [[ "$DEST_HOST" != "local" ]]; then
        verify_ssh "$DEST_HOST" "$SSH_USER"
    fi
    
    # Default destination dataset (same as source dataset by default)
    DEFAULT_DEST_DATASET=$(echo "$SOURCE_DATASET" | sed 's|.*/||')
    DEFAULT_DEST_DATASET="tank/$DEFAULT_DEST_DATASET"
    
    DEST_DATASET=$(select_dataset "$DEST_HOST" "Select destination dataset:" "$DEFAULT_DEST_DATASET" "$SSH_USER")
    if [[ -z "$DEST_DATASET" ]]; then
        error "No destination dataset selected."
    fi
    
    # Recursive option
    if prompt_yes_no "Include child datasets (recursive)?" "y"; then
        RECURSIVE=true
    else
        RECURSIVE=false
    fi
    
    # Check if datasets exist and handle creating new datasets if needed
    has_dest_dataset=true
    if ! has_dataset "$DEST_HOST" "$DEST_DATASET" "$SSH_USER"; then
        log "Destination dataset doesn't exist."
        if prompt_yes_no "Create new dataset on destination?" "y"; then
            log "Will create destination dataset during transfer."
            has_dest_dataset=false
        else
            error "Destination dataset doesn't exist and you chose not to create it. Aborting."
        fi
    fi
    
    # Check for existing snapshots and create if needed
    if $has_dest_dataset; then
        log "Checking for common snapshots..."
        COMMON_SNAPSHOTS=$(find_common_snapshots "$SOURCE_HOST" "$SOURCE_DATASET" "$DEST_HOST" "$DEST_DATASET" "$SSH_USER")
        
        if [[ -z "$COMMON_SNAPSHOTS" ]]; then
            log "No common snapshots found between source and destination."
            if prompt_yes_no "Create initial snapshots on both sides to prepare for future incremental transfers?" "y"; then
                # Create initial snapshots with same name on both sides
                INITIAL_SNAPSHOT="${SNAPSHOT_PREFIX}-initial-$(date +%Y%m%d-%H%M%S)"
                create_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$INITIAL_SNAPSHOT" "$RECURSIVE" "$SSH_USER"
                create_snapshot "$DEST_HOST" "$DEST_DATASET" "$INITIAL_SNAPSHOT" "$RECURSIVE" "$SSH_USER"
                log "Created initial snapshots on both sides. Future transfers will be incremental."
                NEEDS_INITIAL_TRANSFER=false
                SYNC_SNAPSHOT_SRC="$INITIAL_SNAPSHOT"
            else
                log "Will perform full initial transfer."
                NEEDS_INITIAL_TRANSFER=true
            fi
        else
            # Get the first common snapshot, making sure it's properly trimmed
            SYNC_SNAPSHOT_SRC=$(echo "$COMMON_SNAPSHOTS" | head -1 | tr -d '\n\r')
            log "Found common snapshot to use: $SYNC_SNAPSHOT_SRC"
            NEEDS_INITIAL_TRANSFER=false
        fi
    else
        NEEDS_INITIAL_TRANSFER=true
    fi
    
    # Create new snapshot for this transfer
    NEW_SNAPSHOT="${SNAPSHOT_PREFIX}-$(date +%Y%m%d-%H%M%S)"
    create_snapshot "$SOURCE_HOST" "$SOURCE_DATASET" "$NEW_SNAPSHOT" "$RECURSIVE" "$SSH_USER"
    
    # Perform transfer
    if [[ "$NEEDS_INITIAL_TRANSFER" == "true" ]]; then
        log "Performing initial full transfer. This may take a long time..."
        
        # Calculate size for pv
        if [[ "$SOURCE_HOST" == "local" ]]; then
            SIZE=$(zfs list -o used -Hp "$SOURCE_DATASET")
            SIZE_HUMAN=$(zfs list -o used -H "$SOURCE_DATASET")
        else
            SIZE=$(ssh "$SSH_USER@$SOURCE_HOST" "zfs list -o used -Hp '$SOURCE_DATASET'")
            SIZE_HUMAN=$(ssh "$SSH_USER@$SOURCE_HOST" "zfs list -o used -H '$SOURCE_DATASET'")
        fi
        log "Dataset size: $SIZE_HUMAN"
        
        # Set up the replication command
        SEND_CMD="zfs send"
        RECV_CMD="zfs receive"
        
        # Add recursive flag if needed
        if [[ "$RECURSIVE" == "true" ]]; then
            SEND_CMD="$SEND_CMD -R"
        fi
        
        # Add verbose flag
        SEND_CMD="$SEND_CMD -v"
        RECV_CMD="$RECV_CMD -Fuv"
        
        # Perform the transfer
        log "Starting full transfer at $(date)"
        
        # Different command combinations based on source/destination and options
        if [[ "$SOURCE_HOST" == "local" && "$DEST_HOST" == "local" ]]; then
            # Local to local transfer
            $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | pv -petars $SIZE | $RECV_CMD "${DEST_DATASET}"
        elif [[ "$SOURCE_HOST" == "local" ]]; then
            # Local to remote transfer
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                if $RESUME_SUPPORT; then
                    # With compression and resume support
                    $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                      $COMPRESS_CMD | 
                      mbuffer -q -m 1G | 
                      pv -petars $SIZE | 
                      ssh "$SSH_USER@$DEST_HOST" "mbuffer -q -m 1G | $DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
                else
                    # With compression only
                    $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                      $COMPRESS_CMD | 
                      pv -petars $SIZE | 
                      ssh "$SSH_USER@$DEST_HOST" "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
                fi
            else
                if $RESUME_SUPPORT; then
                    # With resume support
                    $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                      mbuffer -q -m 1G | 
                      pv -petars $SIZE | 
                      ssh "$SSH_USER@$DEST_HOST" "mbuffer -q -m 1G | $RECV_CMD '${DEST_DATASET}'"
                else
                    # Basic transfer
                    $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                      pv -petars $SIZE | 
                      ssh "$SSH_USER@$DEST_HOST" "$RECV_CMD '${DEST_DATASET}'"
                fi
            fi
        elif [[ "$DEST_HOST" == "local" ]]; then
            # Remote to local transfer
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                if $RESUME_SUPPORT; then
                    # With compression and resume support
                    ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | $COMPRESS_CMD | mbuffer -q -m 1G" | 
                      pv -petars $SIZE | 
                      mbuffer -q -m 1G | 
                      $DECOMPRESS_CMD | 
                      $RECV_CMD "${DEST_DATASET}"
                else
                    # With compression only
                    ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | $COMPRESS_CMD" | 
                      pv -petars $SIZE | 
                      $DECOMPRESS_CMD | 
                      $RECV_CMD "${DEST_DATASET}"
                fi
            else
                if $RESUME_SUPPORT; then
                    # With resume support
                    ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | mbuffer -q -m 1G" | 
                      pv -petars $SIZE | 
                      mbuffer -q -m 1G | 
                      $RECV_CMD "${DEST_DATASET}"
                else
                    # Basic transfer
                    ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}'" | 
                      pv -petars $SIZE | 
                      $RECV_CMD "${DEST_DATASET}"
                fi
            fi
        else
            # Remote to remote transfer (proxy through local machine)
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | $COMPRESS_CMD" | 
                  pv -petars $SIZE | 
                  ssh "$SSH_USER@$DEST_HOST" "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
            else
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD '${SOURCE_DATASET}@${NEW_SNAPSHOT}'" | 
                  pv -petars $SIZE | 
                  ssh "$SSH_USER@$DEST_HOST" "$RECV_CMD '${DEST_DATASET}'"
            fi
        fi
        
        RESULT=$?
        [[ $RESULT -ne 0 ]] && error "Initial transfer failed with code $RESULT"
        
        # Create sync snapshot for future incremental transfers
        log "Creating sync snapshot for future transfers"
        if [[ "$SOURCE_HOST" == "local" ]]; then
            zfs snapshot -r "${SOURCE_DATASET}@${SYNC_SNAPSHOT}"
        else
            ssh "$SSH_USER@$SOURCE_HOST" "zfs snapshot -r '${SOURCE_DATASET}@${SYNC_SNAPSHOT}'"
        fi
        
        if [[ "$DEST_HOST" == "local" ]]; then
            zfs snapshot -r "${DEST_DATASET}@${SYNC_SNAPSHOT}"
        else
            ssh "$SSH_USER@$DEST_HOST" "zfs snapshot -r '${DEST_DATASET}@${SYNC_SNAPSHOT}'"
        fi
        
    else
        log "Performing incremental transfer..."
        
        # Calculate size for pv (approximate for incremental)
        if [[ "$SOURCE_HOST" == "local" ]]; then
            SIZE_ESTIMATE=$(zfs send -nvP -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | grep "size" | awk '{print $2}')
        else
            SIZE_ESTIMATE=$(ssh "$SSH_USER@$SOURCE_HOST" "zfs send -nvP -i '${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}' '${SOURCE_DATASET}@${NEW_SNAPSHOT}'" | grep "size" | awk '{print $2}')
        fi
        log "Estimated incremental size: $SIZE_ESTIMATE bytes"
        
        # Set up the replication command
        SEND_CMD="zfs send"
        RECV_CMD="zfs receive"
        
        # Add recursive flag if needed
        if [[ "$RECURSIVE" == "true" ]]; then
            SEND_CMD="$SEND_CMD -R"
        fi
        
        # Add verbose flag
        SEND_CMD="$SEND_CMD -v"
        RECV_CMD="$RECV_CMD -Fuv"
        
        # Perform the transfer
        log "Starting incremental transfer at $(date)"
        
        # Different command combinations based on source/destination and options
        if [[ "$SOURCE_HOST" == "local" && "$DEST_HOST" == "local" ]]; then
            # Local to local transfer
            $SEND_CMD -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | pv -pterab | $RECV_CMD "${DEST_DATASET}"
        elif [[ "$SOURCE_HOST" == "local" ]]; then
            # Local to remote transfer
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                $SEND_CMD -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                  $COMPRESS_CMD | 
                  pv -pterab | 
                  ssh "$SSH_USER@$DEST_HOST" "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
            else
                $SEND_CMD -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                  pv -pterab | 
                  ssh "$SSH_USER@$DEST_HOST" "$RECV_CMD '${DEST_DATASET}'"
            fi
        elif [[ "$DEST_HOST" == "local" ]]; then
            # Remote to local transfer
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD -i '${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}' '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | $COMPRESS_CMD" | 
                  pv -pterab | 
                  $DECOMPRESS_CMD | 
                  $RECV_CMD "${DEST_DATASET}"
            else
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD -i '${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}' '${SOURCE_DATASET}@${NEW_SNAPSHOT}'" | 
                  pv -pterab | 
                  $RECV_CMD "${DEST_DATASET}"
            fi
        else
            # Remote to remote transfer (proxy through local machine)
            if $USE_COMPRESSION; then
                if which pigz >/dev/null; then
                    COMPRESS_CMD="pigz"
                    DECOMPRESS_CMD="pigz -d"
                else
                    COMPRESS_CMD="gzip"
                    DECOMPRESS_CMD="gzip -d"
                fi
                
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD -i '${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}' '${SOURCE_DATASET}@${NEW_SNAPSHOT}' | $COMPRESS_CMD" | 
                  pv -pterab | 
                  ssh "$SSH_USER@$DEST_HOST" "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
            else
                ssh "$SSH_USER@$SOURCE_HOST" "$SEND_CMD -i '${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}' '${SOURCE_DATASET}@${NEW_SNAPSHOT}'" | 
                  pv -pterab | 
                  ssh "$SSH_USER@$DEST_HOST" "$RECV_CMD '${DEST_DATASET}'"
            fi
        fi
        
        RESULT=$?
        [[ $RESULT -ne 0 ]] && error "Incremental transfer failed with code $RESULT"
        
        # Update sync snapshot for future incremental transfers
        log "Updating sync snapshot for future transfers"
        if [[ "$SOURCE_HOST" == "local" ]]; then
            zfs destroy "${SOURCE_DATASET}@${SYNC_SNAPSHOT}" 2>/dev/null || true
            zfs rename "${SOURCE_DATASET}@${NEW_SNAPSHOT}" "${SOURCE_DATASET}@${SYNC_SNAPSHOT}"
        else
            ssh "$SSH_USER@$SOURCE_HOST" "zfs destroy '${SOURCE_DATASET}@${SYNC_SNAPSHOT}'" 2>/dev/null || true
            ssh "$SSH_USER@$SOURCE_HOST" "zfs rename '${SOURCE_DATASET}@${NEW_SNAPSHOT}' '${SOURCE_DATASET}@${SYNC_SNAPSHOT}'"
        fi
        
        if [[ "$DEST_HOST" == "local" ]]; then
            zfs destroy "${DEST_DATASET}@${SYNC_SNAPSHOT}" 2>/dev/null || true
            zfs rename "${DEST_DATASET}@${NEW_SNAPSHOT}" "${DEST_DATASET}@${SYNC_SNAPSHOT}"
        else
            ssh "$SSH_USER@$DEST_HOST" "zfs destroy '${DEST_DATASET}@${SYNC_SNAPSHOT}'" 2>/dev/null || true
            ssh "$SSH_USER@$DEST_HOST" "zfs rename '${DEST_DATASET}@${NEW_SNAPSHOT}' '${DEST_DATASET}@${SYNC_SNAPSHOT}'"
        fi
    fi
    
    # Clean up old snapshots
    clean_old_snapshots "$SOURCE_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$SOURCE_HOST" "$SSH_USER"
    clean_old_snapshots "$DEST_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$DEST_HOST" "$SSH_USER"
    
    log "ZFS replication completed successfully at $(date)"
}

# Run main function
main "$@"
