# ZFS Sync Py - Improvement Plan

This document outlines potential improvements for the Python-based `zfs-sync` tool.

## Phase 1: Core Enhancements & Robustness

### 1. Configuration System

*   **Implement YAML Loading:** Replace the current `.env` parsing in `app/config.py` with YAML loading (e.g., using `PyYAML`). This provides better structure for defining multiple complex jobs.
    *   Update `find_config_file` to primarily look for `zfs_sync.yaml`.
    *   Update `load_config` to parse YAML structure.
    *   Update `README.md` with YAML configuration examples.
*   **Configuration Validation:** Use a library like Pydantic to define configuration models (global settings, job settings) for automatic validation, type checking, and clearer error messages on invalid configuration.

### 2. Error Handling & TUI Feedback

*   **Graceful TUI Quit:** Implement the logic in `TransferScreen.action_request_quit` to properly signal the running `syncoid` process (e.g., via SIGTERM/SIGINT if possible) and the worker thread to stop cleanly when the user quits during an active transfer.
*   **SSH Error Handling:** Enhance `execute_command` or the calling functions (`verify_ssh`, `execute_syncoid_transfer`) to potentially catch and interpret specific SSH connection errors (e.g., authentication failure, host unreachable) for more informative user feedback, especially in the TUI.
*   **Syncoid Error Parsing:** In `execute_syncoid_transfer`, attempt to parse common error messages from `syncoid`'s stderr output when it fails, providing more specific reasons for failure than just the exit code in logs/TUI.

### 3. Code Quality & Testing Foundation

*   **Docstrings & Type Hinting:** Perform a pass to ensure all functions, methods, and classes have comprehensive docstrings and accurate type hints.
*   **Basic Unit Tests:** Introduce `pytest` and write initial unit tests for critical, non-TUI functions:
    *   `app/config.py`: Config loading/parsing logic (especially after YAML migration).
    *   `app/utils.py`: `build_sanoid_command`.
    *   `app/transfer.py`: `_parse_zfs_size`, `get_compression_commands`.

## Phase 2: Feature Expansion & Advanced Improvements

### 4. TUI Enhancements

*   **Job Management:** Add TUI screens to allow users to view, create, edit, and delete replication jobs directly within the interactive interface, modifying the underlying configuration file.
*   **Dataset Filtering:** In `DatasetScreen`, add input fields to filter the source and destination dataset lists, improving usability for hosts with many datasets.
*   **Sanoid Policy Viewer:** Add a read-only TUI screen or section that attempts to parse the relevant sections of the configured `sanoid.conf` to display the effective snapshotting and pruning policies for the selected datasets.

### 5. Transfer Options

*   **Direct Remote-to-Remote:** Implement an optional mode for `syncoid` to perform direct remote-to-remote transfers (`ssh source 'zfs send ...' | ssh dest 'zfs receive ...'`). This would require configuration for inter-server SSH keys and modifying the `syncoid` command construction in `perform_transfer`.

### 6. Advanced Testing

*   **Integration Tests:** Develop integration tests that mock `subprocess` calls or use test containers (e.g., Docker) to simulate `zfs`, `ssh`, `sanoid`, and `syncoid` interactions, testing the end-to-end job execution flow (`zfs_sync.run_job`).
*   **TUI Testing:** Explore using `textual-dev` or similar tools to write automated tests for the Textual TUI application flow and component interactions.

### 7. Security Hardening

*   **Review Command Injection Risks:** Double-check all uses of `execute_command` and `subprocess.Popen` to ensure user-provided configuration values (like dataset names, hostnames, SSH options) cannot lead to command injection vulnerabilities, especially if `shell=True` were ever used (which it currently isn't in critical paths). Use `shlex.quote` where appropriate if building shell command strings (though passing argument lists is preferred).
*   **SSH Security Guidance:** Expand the README section on SSH to provide more explicit guidance on secure key generation, permissions, and avoiding insecure options.