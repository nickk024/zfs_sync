#!/bin/bash
set -euo pipefail # Exit on error, unset var, pipe failure
# ZFS Sync - Logging Functions
# --------------------------------

# Standard log message (outputs to stderr for terminal, stdout for file)
log() {
  local message="$(date '+%Y-%m-%d %H:%M:%S') - $*"
  echo "$message" >&2 # Output to stderr for terminal display
  # Only append to log file if it exists and is writable
  if [[ -n "$LOG_FILE" && -w "$(dirname "$LOG_FILE")" ]]; then
    echo "$message" >> "$LOG_FILE" # Still log to file
  fi
}

# Error message and exit
error() {
  local message="$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $*"
  echo "$message" >&2
  # Only append to log file if it exists and is writable
  if [[ -n "$LOG_FILE" && -w "$(dirname "$LOG_FILE")" ]]; then
    echo "$message" >> "$LOG_FILE"
  fi
  exit 1
}

# Debug message (only shown if DEBUG_MODE=true)
debug() {
  if [[ "$DEBUG_MODE" == "true" ]]; then
    local message="$(date '+%Y-%m-%d %H:%M:%S') - DEBUG: $*"
    echo "$message" >&2
    # Only append to log file if it exists and is writable
    if [[ -n "$LOG_FILE" && -w "$(dirname "$LOG_FILE")" ]]; then
      echo "$message" >> "$LOG_FILE"
    fi
  fi
}

# Warning message
warn() {
  local message="$(date '+%Y-%m-%d %H:%M:%S') - WARNING: $*"
  echo "$message" >&2
  # Only append to log file if it exists and is writable
  if [[ -n "$LOG_FILE" && -w "$(dirname "$LOG_FILE")" ]]; then
    echo "$message" >> "$LOG_FILE"
  fi
}
