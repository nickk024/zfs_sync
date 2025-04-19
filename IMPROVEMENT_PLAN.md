# ZFS Sync Script Improvement Plan

This document outlines the planned improvements for the `zfs_sync.sh` script.

## Prioritized Improvements (Phase 1)

### 1. Enhanced Error Handling & Robustness

*   **Pipe Safety:** Implement `set -o pipefail` at the beginning of scripts (especially `transfer.sh`) to ensure pipeline failures are caught correctly.
*   **SSH Resilience:** Add retries or more specific error handling for SSH connection drops or timeouts *during* transfers.
*   **Command Exit Codes:** Explicitly check the exit codes of critical commands (`zfs rename`, `zfs destroy`, etc.) within functions like `setup_sync_snapshot` and `clean_old_snapshots`.
*   **Snapshot Edge Cases:** Handle cases where a dataset exists but has zero snapshots more gracefully (e.g., during transfer type determination or finding common snapshots).
*   **Refine Cleanup:** Make the `cleanup_incomplete_snapshots` pattern more specific (e.g., using a unique prefix like `_incomplete_transfer_`) to avoid accidentally deleting valid user snapshots.
*   **Prerequisite Handling:** Modify `check_prerequisites` to check for required tools (`pv`, `mbuffer`, etc.) and exit with a clear error message instructing the user to install them if missing, instead of attempting auto-installation.
*   **Reinforce `mbuffer`:** Ensure `mbuffer` usage is robust for remote transfers over potentially unstable networks, as discussed.

## Future Enhancements (Phase 2+)

### 2. Improved Efficiency

*   **Direct Remote-to-Remote:** Implement an optional mode for direct `ssh source "zfs send ..." | ssh dest "zfs receive ..."` transfers (requires inter-server SSH keys).
*   **Smarter Resume Logic:** Refine logic to potentially skip `mbuffer` if ZFS native resume (`-t token`) is active.
*   **Reduce SSH Calls:** Explore ways to minimize repeated SSH calls for status checks within a single run.

### 3. Increased Flexibility & Configuration

*   **Multiple Job Support:** Refactor configuration to support defining and selecting multiple replication jobs.
*   **Advanced Snapshot Retention:** Replace `MAX_SNAPSHOTS` with a more flexible retention scheme (e.g., keep X hourly, Y daily, Z weekly).
*   **Bandwidth Limiting:** Add optional configuration to integrate `pv -L RATE` or `trickle` for bandwidth control.
*   **Configurable Sync Snapshot Name:** Make the sync snapshot name (`${SNAPSHOT_PREFIX}-sync`) configurable.

### 4. Code Quality & Maintainability

*   **Refactor Transfer Logic:** Break down large `if/elif/else` blocks in transfer functions into smaller, dynamic pipeline-building functions.
*   **ShellCheck Integration:** Regularly run `shellcheck` on all scripts.
*   **Consistent `local` Usage:** Ensure proper variable scoping with `local`.
*   **Add More Comments:** Enhance comments, especially around complex logic.

### 5. User Experience

*   **Full Dataset Listing:** Modify `select_dataset` to list all datasets or implement filtering.
*   **Dry-Run Mode:** Add a `--dry-run` flag to simulate actions without execution.

### 6. Security & Documentation

*   **README Security Section:** Add documentation emphasizing securing data in transit (VPN/SSH best practices) and clarifying the script's reliance on SSH.
*   **(Optional Future)* Consider `stunnel` integration if needed beyond SSH/VPN.