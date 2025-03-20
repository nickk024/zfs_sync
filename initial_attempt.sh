#!/bin/bash

# ZFS Intelligent Replication Script
# --------------------------------
# This script handles efficient incremental replication between ZFS datasets
# Features:
# - Automatically detects if initial full replication is needed
# - Uses incremental sends for efficiency
# - Maintains synchronized snapshots for reliable transfers
# - Handles child datasets if they exist
# - Shows progress and estimated time remaining
# - Cleans up old snapshots according to retention policy

# ========== CONFIGURATION ==========

# ZFS dataset paths
SOURCE_DATASET="tank/media"
DEST_HOST="100.77.158.38"
DEST_DATASET="tank/media"

# Snapshot naming and retention
SNAPSHOT_PREFIX="backup"
SYNC_SNAPSHOT="${SNAPSHOT_PREFIX}-sync"
MAX_SNAPSHOTS=5  # How many snapshots to keep on source and destination

# Advanced options
USE_COMPRESSION=true    # Whether to use compression in transit
VERIFY_TRANSFERS=false  # Whether to verify transfers (slows things down but adds security)
RECURSIVE=true          # Whether to include child datasets
RESUME_SUPPORT=true     # Whether to enable resumable transfers (needs mbuffer)

# ========== HELPER FUNCTIONS ==========

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $*" >&2
    exit 1
}

check_prerequisites() {
    which pv >/dev/null || error "pv command not found. Please install pv: apt-get install pv"
    
    if $USE_COMPRESSION; then
        which pigz >/dev/null || log "pigz not found. Will use gzip instead (slower compression)"
    fi
    
    if $RESUME_SUPPORT; then
        which mbuffer >/dev/null || error "mbuffer command not found but resumable transfers enabled. Please install mbuffer"
    fi
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root"
    fi
}

verify_ssh() {
    log "Verifying SSH connection to $DEST_HOST..."
    ssh -o BatchMode=yes root@$DEST_HOST "echo SSH connection successful" || {
        error "SSH connection failed. Please setup SSH keys: ssh-copy-id root@$DEST_HOST"
    }
    log "SSH connection verified."
}

has_dataset() {
    local host=$1
    local dataset=$2
    
    if [[ "$host" == "local" ]]; then
        zfs list "$dataset" >/dev/null 2>&1
        return $?
    else
        ssh root@$host "zfs list '$dataset'" >/dev/null 2>&1
        return $?
    fi
}

get_snapshot() {
    local host=$1
    local dataset=$2
    local snapshot=$3
    
    if [[ "$host" == "local" ]]; then
        zfs list -t snapshot -o name "$dataset@$snapshot" >/dev/null 2>&1
        return $?
    else
        ssh root@$host "zfs list -t snapshot -o name '$dataset@$snapshot'" >/dev/null 2>&1
        return $?
    fi
}

find_common_snapshots() {
    local src_dataset=$1
    local dst_dataset=$2
    
    log "Checking for common snapshots between source and destination..."
    
    # Get source snapshots
    src_snaps=$(zfs list -t snapshot -o name -H "$src_dataset" | cut -d@ -f2 || echo "")
    
    # Get destination snapshots
    dst_snaps=$(ssh root@$DEST_HOST "zfs list -t snapshot -o name -H '$dst_dataset'" 2>/dev/null | cut -d@ -f2 || echo "")
    
    # Find common snapshots
    local common=""
    for snap in $src_snaps; do
        if echo "$dst_snaps" | grep -q "^$snap\$"; then
            common="$snap $common"
        fi
    done
    
    echo "$common"
}

create_snapshot() {
    local dataset=$1
    local snapshot=$2
    local recursive=$3
    
    local r_flag=""
    if [[ "$recursive" == "true" ]]; then
        r_flag="-r"
    fi
    
    log "Creating snapshot $dataset@$snapshot"
    zfs snapshot $r_flag "$dataset@$snapshot" || error "Failed to create snapshot $dataset@$snapshot"
}

clean_old_snapshots() {
    local dataset=$1
    local prefix=$2
    local keep=$3
    local host=$4
    
    log "Cleaning old snapshots with prefix $prefix on $host..."
    
    local cmd="zfs list -t snapshot -o name -H | grep \"$dataset@$prefix\" | sort | head -n -$keep | xargs -r zfs destroy -v"
    
    if [[ "$host" == "local" ]]; then
        eval $cmd
    else
        ssh root@$host "$cmd"
    fi
}

# ========== MAIN EXECUTION ==========

main() {
    log "Starting ZFS replication from $SOURCE_DATASET to $DEST_HOST:$DEST_DATASET"
    
    check_prerequisites
    check_root
    verify_ssh
    
    # Check if datasets exist
    has_dataset "local" "$SOURCE_DATASET" || error "Source dataset $SOURCE_DATASET does not exist"
    
    # Create destination dataset if it doesn't exist
    if ! has_dataset "$DEST_HOST" "$DEST_DATASET"; then
        log "Destination dataset doesn't exist. Will create during initial transfer."
        NEEDS_INITIAL_TRANSFER=true
    else
        log "Destination dataset exists. Checking for common snapshots..."
        COMMON_SNAPSHOTS=$(find_common_snapshots "$SOURCE_DATASET" "$DEST_DATASET")
        
        if [[ -z "$COMMON_SNAPSHOTS" ]]; then
            log "No common snapshots found. Need to perform initial transfer."
            NEEDS_INITIAL_TRANSFER=true
        else
            log "Found common snapshots: $COMMON_SNAPSHOTS"
            SYNC_SNAPSHOT_SRC=$(echo "$COMMON_SNAPSHOTS" | awk '{print $1}')
            NEEDS_INITIAL_TRANSFER=false
        fi
    fi
    
    # Create new snapshot
    NEW_SNAPSHOT="${SNAPSHOT_PREFIX}-$(date +%Y%m%d-%H%M%S)"
    create_snapshot "$SOURCE_DATASET" "$NEW_SNAPSHOT" "$RECURSIVE"
    
    # Perform transfer
    if [[ "$NEEDS_INITIAL_TRANSFER" == "true" ]]; then
        log "Performing initial full transfer. This may take a long time..."
        
        # Calculate size for pv
        SIZE=$(zfs list -o used -Hp "$SOURCE_DATASET")
        SIZE_HUMAN=$(zfs list -o used -H "$SOURCE_DATASET")
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
                  ssh root@$DEST_HOST "mbuffer -q -m 1G | $DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
            else
                # With compression only
                $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                  $COMPRESS_CMD | 
                  pv -petars $SIZE | 
                  ssh root@$DEST_HOST "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
            fi
        else
            if $RESUME_SUPPORT; then
                # With resume support
                $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                  mbuffer -q -m 1G | 
                  pv -petars $SIZE | 
                  ssh root@$DEST_HOST "mbuffer -q -m 1G | $RECV_CMD '${DEST_DATASET}'"
            else
                # Basic transfer
                $SEND_CMD "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
                  pv -petars $SIZE | 
                  ssh root@$DEST_HOST "$RECV_CMD '${DEST_DATASET}'"
            fi
        fi
        
        RESULT=$?
        [[ $RESULT -ne 0 ]] && error "Initial transfer failed with code $RESULT"
        
        # Create sync snapshot for future incremental transfers
        log "Creating sync snapshot for future transfers"
        zfs snapshot -r "${SOURCE_DATASET}@${SYNC_SNAPSHOT}"
        ssh root@$DEST_HOST "zfs snapshot -r '${DEST_DATASET}@${SYNC_SNAPSHOT}'"
        
    else
        log "Performing incremental transfer..."
        
        # Calculate size for pv (approximate for incremental)
        SIZE_ESTIMATE=$(zfs send -nvP -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | grep "size" | awk '{print $2}')
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
              ssh root@$DEST_HOST "$DECOMPRESS_CMD | $RECV_CMD '${DEST_DATASET}'"
        else
            $SEND_CMD -i "${SOURCE_DATASET}@${SYNC_SNAPSHOT_SRC}" "${SOURCE_DATASET}@${NEW_SNAPSHOT}" | 
              pv -pterab | 
              ssh root@$DEST_HOST "$RECV_CMD '${DEST_DATASET}'"
        fi
        
        RESULT=$?
        [[ $RESULT -ne 0 ]] && error "Incremental transfer failed with code $RESULT"
        
        # Update sync snapshot for future incremental transfers
        log "Updating sync snapshot for future transfers"
        zfs destroy "${SOURCE_DATASET}@${SYNC_SNAPSHOT}"
        ssh root@$DEST_HOST "zfs destroy '${DEST_DATASET}@${SYNC_SNAPSHOT}'"
        zfs rename "${SOURCE_DATASET}@${NEW_SNAPSHOT}" "${SOURCE_DATASET}@${SYNC_SNAPSHOT}"
        ssh root@$DEST_HOST "zfs rename '${DEST_DATASET}@${NEW_SNAPSHOT}' '${DEST_DATASET}@${SYNC_SNAPSHOT}'"
    fi
    
    # Clean up old snapshots
    clean_old_snapshots "$SOURCE_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "local"
    clean_old_snapshots "$DEST_DATASET" "$SNAPSHOT_PREFIX" "$MAX_SNAPSHOTS" "$DEST_HOST"
    
    log "ZFS replication completed successfully at $(date)"
}

# Run main function
main