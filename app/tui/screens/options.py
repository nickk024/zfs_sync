import logging
from typing import Dict, Any

from textual.app import ComposeResult
from textual.containers import VerticalScroll, Container # Import Container
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Button, Input, Label, Checkbox
from textual.binding import Binding
from textual.validation import Integer

# Import next screen
from .summary import SummaryScreen

class OptionsScreen(Screen):
    """Screen for setting transfer and snapshot options."""
    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=True),
        Binding("ctrl+q", "app.quit", "Quit", show=False),
    ]

    def __init__(self, config: Dict[str, Any], job_config_so_far: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.job_config_so_far = job_config_so_far

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="options-form", border_title="Options"): # Outer container
            with VerticalScroll(): # Inner scroll
                yield Static("Transfer Options:", classes="section-title")
                yield Checkbox("Recursive transfer",
                           value=self.config.get('DEFAULT_RECURSIVE', True),
                           id="check-recursive")
                yield Checkbox("Use compression during transfer",
                           value=self.config.get('DEFAULT_USE_COMPRESSION', True),
                           id="check-compression")
                yield Checkbox("Enable resume support for initial transfer",
                           value=self.config.get('DEFAULT_RESUME_SUPPORT', True),
                           id="check-resume")

                yield Static("Snapshot Options:", classes="section-title")
                yield Label("Snapshot Prefix:")
                yield Input(value=self.config.get('DEFAULT_SNAPSHOT_PREFIX', 'zfs-sync'),
                            id="input-prefix")
                yield Label("Max Snapshots to keep on destination:")
                yield Input(value=str(self.config.get('DEFAULT_MAX_SNAPSHOTS', 5)),
                            id="input-max-snapshots",
                            validators=[Integer(minimum=0, failure_description="Must be a non-negative number.")])

                yield Static("Execution Options:", classes="section-title")
                yield Checkbox("Perform a dry run (show commands without executing)?",
                           value=False, # Default dry run to False for interactive
                           id="check-dry-run")

                yield Static() # Spacer
                yield Button("Review & Start", variant="primary", id="button-continue")
                yield Static(id="status-message", classes="status-hidden")
# Removed duplicated block from previous incorrect diff
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "button-continue":
            # Validate inputs
            max_snap_input = self.query_one("#input-max-snapshots", Input)
            if not max_snap_input.is_valid:
                self.query_one("#status-message").update("[red]Invalid number for Max Snapshots.[/]")
                self.query_one("#status-message").remove_class("status-hidden")
                max_snap_input.focus()
                return
            else:
                 self.query_one("#status-message").add_class("status-hidden") # Clear error

            # Gather values
            self.job_config_so_far['recursive'] = self.query_one("#check-recursive", Checkbox).value
            self.job_config_so_far['use_compression'] = self.query_one("#check-compression", Checkbox).value
            self.job_config_so_far['resume_support'] = self.query_one("#check-resume", Checkbox).value
            self.job_config_so_far['snapshot_prefix'] = self.query_one("#input-prefix", Input).value.strip()
            self.job_config_so_far['max_snapshots'] = int(max_snap_input.value)
            self.job_config_so_far['dry_run'] = self.query_one("#check-dry-run", Checkbox).value
            # Add derived/fixed values
            self.job_config_so_far['_job_name'] = 'interactive'
            self.job_config_so_far['sync_snapshot'] = f"{self.job_config_so_far['snapshot_prefix']}-sync"
            self.job_config_so_far['direct_remote_transfer'] = False # Hardcoded

            # Proceed to Summary Screen
            self.app.push_screen(SummaryScreen(config=self.config, job_config=self.job_config_so_far))