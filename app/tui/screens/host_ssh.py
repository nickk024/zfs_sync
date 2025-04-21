import logging
from typing import Dict, Any, Optional # Added Optional
import json # Needed for saving state
from pathlib import Path # Needed for saving state

from textual.app import ComposeResult
from textual.containers import VerticalScroll, Container # Import Container
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Button, Input, Label, LoadingIndicator
from textual.binding import Binding
from textual.worker import Worker, WorkerState # Import Worker and WorkerState
from textual.reactive import reactive # Import reactive for the worker attribute

# Assuming these are moved or accessible
# from ..app import ZFSSyncApp # Or however the app instance is accessed
from ...utils import verify_ssh # Imports from lib/utils.py
from ..constants import STATE_FILE_PATH # Import from constants
from .dataset import DatasetScreen # Import next screen

# --- Utility function moved here to break circular import ---
def _save_interactive_state(state_file: Path, state: Dict[str, Any]):
    """Saves the interactive state to a JSON file."""
    try:
        with state_file.open('w') as f:
            json.dump(state, f, indent=4)
            logging.debug(f"Saved interactive state to {state_file}: {state}")
    except OSError as e:
        logging.error(f"Could not save state file {state_file}: {e}")

class HostSSHScreen(Screen):
    """Screen for entering Host and SSH details."""

    worker: reactive[Optional[Worker]] = reactive(None) # Add worker attribute
    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=False),
        Binding("ctrl+q", "app.quit", "Quit", show=False),
    ]

    def __init__(self, config: Dict[str, Any], initial_state: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.initial_state = initial_state
        self.job_config_so_far: Dict[str, Any] = {} # To store selections

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="setup-form"): # Outer container with ID
            with VerticalScroll(): # Inner scroll container
                # Widgets yielded inside VerticalScroll
                yield Label("Source Host:")
                yield Input(
                    value=self.initial_state.get('last_src_host', self.config.get('DEFAULT_SOURCE_HOST', 'local')),
                    id="input-src-host"
                )
                yield Label("Destination Host:")
                yield Input(
                    value=self.initial_state.get('last_dst_host', self.config.get('DEFAULT_DEST_HOST', '')),
                    placeholder="e.g., backup-server.local or user@host",
                    id="input-dst-host"
                )
                yield Label("SSH User:")
                yield Input(
                    value=self.initial_state.get('last_ssh_user', self.config.get('DEFAULT_SSH_USER', 'root')),
                    id="input-ssh-user"
                )
                yield Static() # Spacer
                yield Button("Verify & Continue", variant="primary", id="button-continue")
                yield Static(id="status-message", classes="status-hidden") # For status/error messages
                yield LoadingIndicator(id="loading-indicator", classes="status-hidden") # Hidden initially
        yield Footer()

    def on_mount(self) -> None:
        """Set the border title after the widget is mounted."""
        try:
            container = self.query_one("#setup-form", Container)
            container.border_title = "Host & SSH"
        except Exception as e:
            logging.error(f"Error setting border title for #setup-form: {e}") # Add logging

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "button-continue":
            src_host_input = self.query_one("#input-src-host", Input)
            dst_host_input = self.query_one("#input-dst-host", Input)
            ssh_user_input = self.query_one("#input-ssh-user", Input)
            status_label = self.query_one("#status-message", Static)
            loading = self.query_one("#loading-indicator", LoadingIndicator)

            src_host = src_host_input.value.strip()
            dst_host = dst_host_input.value.strip()
            ssh_user = ssh_user_input.value.strip()

            if not dst_host:
                status_label.update("[red]Destination Host cannot be empty.[/]")
                status_label.remove_class("status-hidden")
                return

            # Store values for next screen
            self.job_config_so_far = {
                'source_host': src_host,
                'dest_host': dst_host,
                'ssh_user': ssh_user,
            }

            # Disable inputs/button, show loading
            src_host_input.disabled = True
            dst_host_input.disabled = True
            ssh_user_input.disabled = True
            event.button.disabled = True
            status_label.update("Verifying SSH connections...")
            status_label.remove_class("status-hidden")
            loading.remove_class("status-hidden")

            # --- Perform Verification (potentially long running) ---
            # Store the worker so we can watch its state
            self.worker = self.app.run_worker(
                self.verify_connections(src_host, dst_host, ssh_user),
                # No callback here
                exclusive=True,
                group="ssh_verify" # Optional: group the worker
            )

    async def verify_connections(self, src_host: str, dst_host: str, ssh_user: str) -> tuple[bool, bool, str]: # Make async again, return results
        """Worker function to verify SSH connections. Returns (src_ok, dst_ok, error_msg)."""
        src_ok = False
        dst_ok = False
        error_msg = ""

        try:
            # status_widget = self.query_one("#status-message", Static) # No need to get widget here
            # Verify source
            # No UI updates from here directly, use run_sync_in_worker_thread for blocking calls
            src_ok = await self.app.run_sync_in_worker_thread( # Use await and run_sync
                 verify_ssh, src_host, ssh_user, self.config,
                 thread_name=f"verify_ssh_{src_host}" # Optional: name the thread
            )

            if not src_ok:
                error_msg = f"Failed to connect to Source Host: {src_host}"
            else:
                # Verify destination (only if different from source)
                if src_host == dst_host:
                    dst_ok = True
                    self.app.call_from_thread(status_widget.update, "Source & Destination are the same.")
                else:
                    # No UI updates from here directly
                    # Use await and run_sync_in_worker_thread for blocking calls
                    dst_ok = await self.app.run_sync_in_worker_thread( # Use await and run_sync
                        verify_ssh, dst_host, ssh_user, self.config,
                        thread_name=f"verify_ssh_{dst_host}" # Optional: name the thread
                    )
                    if not dst_ok:
                        error_msg = f"Failed to connect to Destination Host: {dst_host}"

        except Exception as e:
            error_msg = f"Error during SSH verification: {e}"
            logging.exception("SSH Verification Error")

        # --- Return results for the callback ---
        return src_ok, dst_ok, error_msg

    def update_verification_status(self, result: tuple[bool, bool, str]) -> None: # Accept worker result tuple
        """Callback to update UI after verification worker finishes."""
        src_ok, dst_ok, error_msg = result # Unpack the results
        status_label = self.query_one("#status-message", Static)
        loading = self.query_one("#loading-indicator", LoadingIndicator)
        button = self.query_one("#button-continue", Button)
        src_host_input = self.query_one("#input-src-host", Input)
        dst_host_input = self.query_one("#input-dst-host", Input)
        ssh_user_input = self.query_one("#input-ssh-user", Input)

        loading.add_class("status-hidden") # Hide loading indicator

        if src_ok and dst_ok:
            status_label.update("[green]SSH connections verified successfully![/]")
            # Save state
            current_state = {
                'last_src_host': self.job_config_so_far['source_host'],
                'last_dst_host': self.job_config_so_far['dest_host'],
                'last_ssh_user': self.job_config_so_far['ssh_user'],
            }
            _save_interactive_state(STATE_FILE_PATH, current_state)

            # Proceed to the next screen
            self.app.push_screen(DatasetScreen(config=self.config, job_config_so_far=self.job_config_so_far))

        else:
            status_label.update(f"[red]Verification Failed: {error_msg}[/]")
            # Re-enable inputs/button to allow correction
            button.disabled = False

    def watch_worker(self, worker: Optional[Worker]) -> None:
        """Called when the worker attribute changes.

        This method will update the UI based on the worker state.
        """
        if worker is None:
            # Worker has finished or hasn't started
            return

        # Update UI based on worker state
        if worker.state == WorkerState.PENDING:
            # Optional: Update UI to show pending state if needed
            pass
        elif worker.state == WorkerState.RUNNING:
            # Optional: Update UI to show running state if needed
            pass
        elif worker.state == WorkerState.SUCCESS:
            # Worker finished successfully, get result and update UI
            result = worker.result
            self.update_verification_status(result)
            self.worker = None # Clear the worker attribute
        elif worker.state == WorkerState.ERROR:
            # Worker failed, log error and update UI
            logging.error(f"SSH verification worker failed: {worker.error}")
            # Pass a failure state to the UI update method
            # Assuming result format is (bool, bool, str)
            self.update_verification_status((False, False, str(worker.error)))
            self.worker = None # Clear the worker attribute
        elif worker.state == WorkerState.CANCELLED:
            # Optional: Handle cancellation if needed
            logging.info("SSH verification worker cancelled.")
            # Update UI to show cancellation
            status_label = self.query_one("#status-message", Static)
            loading = self.query_one("#loading-indicator", LoadingIndicator)
            button = self.query_one("#button-continue", Button)
            src_host_input = self.query_one("#input-src-host", Input)
            dst_host_input = self.query_one("#input-dst-host", Input)
            ssh_user_input = self.query_one("#input-ssh-user", Input)

            loading.add_class("status-hidden")
            status_label.update("[yellow]Verification Cancelled.[/]")
            button.disabled = False
            src_host_input.disabled = False
            dst_host_input.disabled = False
            ssh_user_input.disabled = False
            self.worker = None # Clear the worker attribute

            src_host_input.disabled = False
            dst_host_input.disabled = False
            ssh_user_input.disabled = False