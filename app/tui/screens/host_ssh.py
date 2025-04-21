import logging
from typing import Dict, Any

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Button, Input, Label, LoadingIndicator
from textual.binding import Binding

# Assuming these are moved or accessible
# from ..app import ZFSSyncApp # Or however the app instance is accessed
from ...utils import verify_ssh # Imports from lib/utils.py
from ..app import _save_interactive_state, STATE_FILE_PATH # Import TUI utils from app.py
from .dataset import DatasetScreen # Import next screen

class HostSSHScreen(Screen):
    """Screen for entering Host and SSH details."""
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
        with VerticalScroll(id="setup-form"):
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
            self.app.run_worker(
                self.verify_connections(src_host, dst_host, ssh_user),
                exclusive=True
            )

    async def verify_connections(self, src_host: str, dst_host: str, ssh_user: str) -> None:
        """Worker function to verify SSH connections."""
        src_ok = False
        dst_ok = False
        error_msg = ""

        try:
            status_widget = self.query_one("#status-message", Static) # Get widget ref before thread switch
            # Verify source
            self.app.call_from_thread(status_widget.update, f"Verifying SSH to Source ({src_host})...")
            src_ok = await self.app.run_sync_in_worker_thread(
                 verify_ssh, src_host, ssh_user, self.config
            )

            if not src_ok:
                error_msg = f"Failed to connect to Source Host: {src_host}"
            else:
                # Verify destination (only if different from source)
                if src_host == dst_host:
                    dst_ok = True
                    self.app.call_from_thread(status_widget.update, "Source & Destination are the same.")
                else:
                    self.app.call_from_thread(status_widget.update, f"Verifying SSH to Destination ({dst_host})...")
                    dst_ok = await self.app.run_sync_in_worker_thread(
                        verify_ssh, dst_host, ssh_user, self.config
                    )
                    if not dst_ok:
                        error_msg = f"Failed to connect to Destination Host: {dst_host}"

        except Exception as e:
            error_msg = f"Error during SSH verification: {e}"
            logging.exception("SSH Verification Error")

        # --- Update UI from Worker ---
        self.app.call_from_thread(self.update_verification_status, src_ok, dst_ok, error_msg)

    def update_verification_status(self, src_ok: bool, dst_ok: bool, error_msg: str) -> None:
        """Callback to update UI after verification worker finishes."""
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
            src_host_input.disabled = False
            dst_host_input.disabled = False
            ssh_user_input.disabled = False