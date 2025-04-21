import logging
import threading
import os # Import os
import shlex # Import shlex
from datetime import datetime
from typing import Optional, Dict, Any

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import Screen
from textual.widgets import Header, Footer, Log, ProgressBar, Static
from textual.reactive import reactive
from textual.binding import Binding

# Import messages and necessary functions
from ..messages import LogMessage, ProgressUpdate, TransferFinished
from ...zfs import has_dataset # Keep has_dataset for checks
# Import the new unified transfer function
from ...transfer import perform_transfer
# Import execute_command for sanoid helper
from ...utils import execute_command, build_sanoid_command # Import new helper

# --- Helper to run Sanoid (Async version for TUI worker) ---

async def run_sanoid_command_async(screen_instance: 'TransferScreen', action: str, host: str, ssh_user: Optional[str], run_config: Dict[str, Any]) -> bool:
    """Helper function to execute sanoid commands asynchronously using the centralized builder."""
    dry_run = run_config.get('DRY_RUN', False) # Still need dry_run for logging logic here

    try:
        cmd = build_sanoid_command(action, run_config)
    except ValueError as e:
        screen_instance.log_message(str(e), level=logging.ERROR)
        return False

    # Determine log action string based on the original action requested
    if action == "take":
        log_action = "Taking snapshots"
    elif action == "prune":
        log_action = "Pruning snapshots"
    else:
        # This case should not be reached if build_sanoid_command raises ValueError
        log_action = f"action '{action}'"

        screen_instance.log_message(f"[DRY RUN] Would execute sanoid on {host}: {' '.join(shlex.quote(c) for c in cmd)}")
        try:
            # Run async via worker thread pool
            await screen_instance.app.run_sync_in_worker_thread(
                execute_command, cmd, host=host, ssh_user=ssh_user, config=run_config, check=False, capture_output=True
            )
        except Exception as e:
             screen_instance.log_message(f"[DRY RUN] Error simulating sanoid command (continuing): {e}", level=logging.WARNING)
        return True # Assume success in dry run

    screen_instance.log_message(f"Executing sanoid on {host}: {log_action}...")
    try:
        # Run async via worker thread pool
        result = await screen_instance.app.run_sync_in_worker_thread(
             execute_command, cmd, host=host, ssh_user=ssh_user, config=run_config, check=True, capture_output=True
        )
        screen_instance.log_message(f"Sanoid stdout:\n{result.stdout}", level=logging.DEBUG)
        if result.stderr:
             screen_instance.log_message(f"Sanoid stderr:\n{result.stderr}", level=logging.WARNING)
        screen_instance.log_message(f"Sanoid {log_action} completed successfully on {host}.")
        return True
    except Exception as e:
        # Error logged by execute_command wrapper or run_sync_in_worker_thread
        screen_instance.log_message(f"Sanoid {log_action} failed on {host}.", level=logging.ERROR)
        # Log the exception details as well
        logging.exception(f"Sanoid {log_action} failed on {host}")
        return False


class TransferScreen(Screen):
    """Screen to display transfer progress and logs."""
    BINDINGS = [
        # Allow quitting, but maybe add confirmation later if transfer is running
        Binding("ctrl+c", "request_quit", "Quit", show=True),
        Binding("ctrl+q", "request_quit", "Quit", show=False),
    ]

    # Reactive variables to update the UI
    current_action = reactive("Initializing Transfer...")
    total_bytes = reactive[Optional[int]](None)
    completed_bytes = reactive(0)
    transfer_finished = reactive(False)
    transfer_success = reactive(False)

    def __init__(self, job_config: Dict[str, Any], **kwargs):
         super().__init__(**kwargs)
         self.job_config = job_config
         self._stop_event = threading.Event() # Event to signal worker thread to stop

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="transfer-container"):
            self.query_one("#transfer-container", Container).border_title = "Transfer Progress"
            # Log area should take up most space
            yield Log(id="log-area", highlight=True, markup=True)
            yield ProgressBar(id="transfer-progress", total=100, show_eta=True)
            yield Static(id="final-status", classes="status-hidden") # For final success/failure message
        yield Footer()

    def on_mount(self) -> None:
        """Start the transfer worker when the screen is mounted."""
        self.query_one(Log).write_line("Transfer screen mounted. Starting worker...")
        self.update_progress_display() # Set initial state
        # Start the transfer logic in a worker thread
        self.app.run_worker(
            self.run_transfer_worker,
            exclusive=True,
            group="transfer"
        )

    def action_request_quit(self) -> None:
        """Handle quit request during transfer."""
        # TODO: Implement logic to gracefully stop the transfer worker
        # For now, just notify and quit
        self.app.notify("Quit requested. Stopping transfer (placeholder)...", severity="warning")
        self._stop_event.set() # Signal the worker thread to stop if possible
        # Maybe wait briefly for worker?
        self.app.exit() # Exit the app

    # --- Worker ---
    async def run_transfer_worker(self) -> None:
        """The worker thread that executes the main transfer logic using sanoid and syncoid."""
        success = False
        try:
            self.log_message("Worker started. Preparing transfer...")

            # Combine global config with job config
            global_config_keys = ['SSH_TIMEOUT', 'LOG_LEVEL', 'LOG_FILE', 'DRY_RUN', 'SSH_EXTRA_OPTIONS', 'SANOID_PATH', 'SANOID_CONF_PATH', 'SYNCOID_PATH'] # Add sanoid/syncoid paths
            run_config = {k: self.app.config.get(k) for k in global_config_keys if self.app.config.get(k) is not None}
            run_config.update(self.job_config) # Overlay job config

            # --- Pre-flight Dataset Checks ---
            src_host = self.job_config['source_host']
            src_dataset = self.job_config['source_dataset']
            dst_host = self.job_config['dest_host']
            dst_dataset = self.job_config['dest_dataset']
            ssh_user = self.job_config['ssh_user']

            self.log_message(f"Checking source dataset {src_dataset} on {src_host}...")
            src_exists = await self.app.run_sync_in_worker_thread(
                has_dataset, src_dataset, src_host, ssh_user, run_config
            )
            if not src_exists:
                 raise Exception(f"Source dataset {src_dataset} not found on {src_host}.")
            self.log_message("Source dataset confirmed.")

            self.log_message(f"Checking destination dataset {dst_dataset} on {dst_host}...")
            dest_exists = await self.app.run_sync_in_worker_thread(
                has_dataset, dst_dataset, dst_host, ssh_user, run_config
            )
            if dest_exists:
                self.log_message("Destination dataset exists.")
            else:
                self.log_message("Destination dataset does not exist. Syncoid will create it.")

            # --- Take Snapshots using Sanoid ---
            self.log_message("Taking source snapshots via sanoid...")
            snap_success = await run_sanoid_command_async(
                self, "take", src_host, ssh_user if src_host != "local" else None, run_config
            )
            if not snap_success:
                 raise Exception("Failed to take snapshots on source using sanoid.")
            self.log_message("Sanoid snapshot creation finished.")

            # --- Perform Transfer using Syncoid ---
            self.log_message("Starting syncoid transfer...")
            # Pass self (the screen instance) as the 'app' argument to the transfer function
            success = await self.app.run_sync_in_worker_thread(
                perform_transfer, self.job_config, self.app.config, self # Pass screen instance
            )

            # --- Prune Snapshots using Sanoid (on Destination) ---
            if success and not run_config.get('DRY_RUN', False):
                self.log_message("Transfer successful. Pruning snapshots on destination...")
                prune_success = await run_sanoid_command_async(
                    self, "prune", dst_host, ssh_user if dst_host != "local" else None, run_config
                )
                if not prune_success:
                    self.log_message("Destination snapshot pruning failed.", level=logging.WARNING)
                    # Don't mark the whole job as failed for pruning failure
            elif not success:
                 self.log_message("Transfer failed. Skipping snapshot pruning.", level=logging.ERROR)
            elif run_config.get('DRY_RUN', False):
                 self.log_message("[DRY RUN] Skipping sanoid prune.")


        except Exception as e:
            self.log_message(f"Transfer worker error: {e}", level=logging.ERROR)
            logging.exception("Error in transfer worker") # Log full traceback
            success = False
        finally:
            # Signal that the transfer is finished, regardless of success/failure
            self.app.call_from_thread(self.post_message, TransferFinished(success))


    # --- Message Handlers for UI Updates ---
    def on_log_message(self, message: LogMessage) -> None:
        """Handle LogMessage to add text to the log widget."""
        try:
            log_widget = self.query_one(Log)
            # Simple level mapping for color (can be expanded)
            color = "white"
            prefix = "[INFO ]"
            if message.level == logging.DEBUG:
                color = "dim"
                prefix = "[DEBUG]"
            elif message.level == logging.WARNING:
                color = "yellow"
                prefix = "[WARN ]"
            elif message.level >= logging.ERROR:
                color = "red"
                prefix = "[ERROR]"

            log_widget.write_line(f"[{color}]{prefix} {message.text}[/]")
        except Exception as e:
             logging.error(f"Error writing to TUI log widget: {e}") # Log errors updating UI

    def on_progress_update(self, message: ProgressUpdate) -> None:
        """Handle ProgressUpdate to update the progress bar."""
        if message.action:
            self.current_action = message.action # Update reactive var, watcher will update widget
        self.total_bytes = message.total
        self.completed_bytes = message.completed
        # Watchers for total_bytes and completed_bytes will call update_progress_display

    def on_transfer_finished(self, message: TransferFinished) -> None:
        """Handle TransferFinished message."""
        self.transfer_finished = True
        self.transfer_success = message.success
        try:
            final_status_widget = self.query_one("#final-status")
            progress_bar = self.query_one(ProgressBar)
            if message.success:
                final_status_widget.update("[bold green]Transfer Completed Successfully![/]")
                progress_bar.border_title = "Complete"
            else:
                final_status_widget.update("[bold red]Transfer Failed![/]")
                progress_bar.border_title = "Failed"
            final_status_widget.remove_class("status-hidden")
            # Maybe add a "Close" button or automatically pop the screen after a delay?
            # For now, user can quit with Ctrl+C
        except Exception as e:
             logging.error(f"Error updating final status widget: {e}")

    # --- Reactive Watchers ---
    def watch_current_action(self, action: str) -> None:
        """Update the progress bar description."""
        # Ensure progress bar exists before updating
        try:
            progress_bar = self.query_one(ProgressBar)
            progress_bar.border_title = action
        except Exception:
            pass # Widget might not be mounted yet

    def watch_total_bytes(self, total: Optional[int]) -> None:
        """Update the progress bar total."""
        self.update_progress_display()

    def watch_completed_bytes(self, completed: int) -> None:
        """Update the progress bar completed value."""
        self.update_progress_display()

    # --- Helper Methods ---
    def log_message(self, text: str, level: int = logging.INFO) -> None:
        """Helper to post a LogMessage."""
        # Ensure message is posted from the correct thread context if needed
        # Since this is called from the worker, use call_from_thread
        self.app.call_from_thread(self.post_message, LogMessage(text, level))

    def update_progress(self, completed: int, total: Optional[int], action: Optional[str] = None) -> None:
        """Helper to post a ProgressUpdate message."""
         # Ensure message is posted from the correct thread context if needed
        self.app.call_from_thread(self.post_message, ProgressUpdate(completed, total, action))

    def update_progress_display(self) -> None:
        """Updates the visual progress bar based on reactive variables."""
        try:
            progress_bar = self.query_one(ProgressBar)
            if self.total_bytes is not None and self.total_bytes > 0:
                progress_bar.total = self.total_bytes
                progress_bar.progress = self.completed_bytes
            else:
                # Indeterminate state
                progress_bar.total = None # Setting total to None makes it indeterminate
                progress_bar.progress = 0 # Reset progress value
        except Exception:
            pass # Widget might not be mounted yet