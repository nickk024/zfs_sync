import logging
from typing import Dict, Any

from textual.app import ComposeResult
from textual.containers import VerticalScroll, Horizontal
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Button, DataTable
from textual.binding import Binding

# Import message to post
from ..messages import StartTransfer

class SummaryScreen(Screen):
    """Screen to display summary and confirm before starting."""
    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=True),
        Binding("ctrl+q", "app.quit", "Quit", show=False),
    ]

    def __init__(self, config: Dict[str, Any], job_config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.job_config = job_config

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="summary-container", border_title="Summary & Confirmation"): # Outer container
            with VerticalScroll(): # Inner scroll
                yield Static("Configuration Summary", classes="section-title")
                # Use DataTable for a structured summary
                yield DataTable(id="summary-table", show_header=False, show_cursor=False)
                yield Static() # Spacer
                yield Horizontal(
                    Button("Back", id="button-back"),
                    Button("Start Sync", variant="primary", id="button-start"),
                    id="summary-buttons"
                )
                yield Static(id="status-message", classes="status-hidden")
            yield DataTable(id="summary-table", show_header=False, show_cursor=False)
            yield Static() # Spacer
            yield Horizontal(
                Button("Back", id="button-back"),
                Button("Start Sync", variant="primary", id="button-start"),
                id="summary-buttons"
            )
            yield Static(id="status-message", classes="status-hidden")
        yield Footer()

    def on_mount(self) -> None:
        """Populate the summary table."""
        table = self.query_one(DataTable)
        table.add_columns("Parameter", "Value")

        # Helper to format boolean values
        def format_bool(value: bool) -> str:
            return "[green]Yes[/]" if value else "[red]No[/]"

        # Populate rows
        rows = [
            ("Source", f"{self.job_config['ssh_user']}@{self.job_config['source_host']}:{self.job_config['source_dataset']}"),
            ("Destination", f"{self.job_config['ssh_user']}@{self.job_config['dest_host']}:{self.job_config['dest_dataset']}"),
            ("Recursive", format_bool(self.job_config['recursive'])),
            ("Compression", format_bool(self.job_config['use_compression'])),
            ("Resume Support", format_bool(self.job_config['resume_support'])),
            ("Snapshot Prefix", self.job_config['snapshot_prefix']),
            ("Max Snapshots", str(self.job_config['max_snapshots'])),
            ("Dry Run", format_bool(self.job_config['dry_run'])),
        ]
        for row in rows:
            # Add markup=True if needed, but DataTable handles Rich markup in cells
            table.add_row(row[0], row[1])

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "button-back":
            self.app.pop_screen()
        elif event.button.id == "button-start":
            status = self.query_one("#status-message")
            if self.job_config['dry_run']:
                status.update("[yellow]Posting StartTransfer message (Dry Run)...[/]")
            else:
                status.update("[green]Posting StartTransfer message...[/]")
            status.remove_class("status-hidden")
            event.button.disabled = True
            self.query_one("#button-back").disabled = True

            # Post a message that the main app will handle to start the transfer
            self.app.post_message(StartTransfer(self.job_config))