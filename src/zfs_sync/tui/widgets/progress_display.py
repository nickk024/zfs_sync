"""
Progress Display Widget

This module provides a widget for displaying progress of ZFS synchronization operations.
"""

import logging
import time
from typing import Optional, Dict, Any, List

from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.widgets import Static, ProgressBar, Label, Log
from textual.reactive import reactive

logger = logging.getLogger('zfs_sync.tui.widgets.progress_display')

class ProgressDisplay(Vertical):
    """Widget for displaying progress of ZFS synchronization operations."""
    
    status = reactive("Idle")
    progress = reactive(0.0)
    current_operation = reactive("")
    start_time = reactive(0.0)
    end_time = reactive(0.0)
    
    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> None:
        """
        Initialize the progress display widget.
        
        Args:
            id: Widget ID
            name: Widget name
        """
        super().__init__(id=id, name=name)
    
    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        with Horizontal(classes="progress-header"):
            yield Label("Status:", classes="progress-label")
            yield Static("Idle", id="status-text", classes="progress-value")
        
        with Horizontal(classes="progress-row"):
            yield Label("Operation:", classes="progress-label")
            yield Static("", id="operation-text", classes="progress-value")
        
        with Horizontal(classes="progress-row"):
            yield Label("Progress:", classes="progress-label")
            yield ProgressBar(id="progress-bar", show_percentage=True)
        
        with Horizontal(classes="progress-row"):
            yield Label("Elapsed Time:", classes="progress-label")
            yield Static("00:00:00", id="elapsed-time", classes="progress-value")
        
        with Horizontal(classes="progress-row"):
            yield Label("Estimated Time:", classes="progress-label")
            yield Static("--:--:--", id="estimated-time", classes="progress-value")
        
        yield Static("Log Output:", classes="log-header")
        yield Log(id="progress-log", highlight=True, markup=True)
    
    def on_mount(self) -> None:
        """Called when the widget is mounted."""
        self.set_interval(1.0, self.update_elapsed_time)
    
    def update_elapsed_time(self) -> None:
        """Update the elapsed time display."""
        if self.status == "Running" and self.start_time > 0:
            elapsed = time.time() - self.start_time
            elapsed_text = self.format_time(elapsed)
            
            # Update elapsed time display
            elapsed_time = self.query_one("#elapsed-time", Static)
            elapsed_time.update(elapsed_text)
            
            # Update estimated time display if progress > 0
            if self.progress > 0:
                estimated_total = elapsed / self.progress
                estimated_remaining = estimated_total - elapsed
                
                if estimated_remaining > 0:
                    estimated_text = self.format_time(estimated_remaining)
                    estimated_time = self.query_one("#estimated-time", Static)
                    estimated_time.update(estimated_text)
    
    def format_time(self, seconds: float) -> str:
        """
        Format time in seconds to HH:MM:SS format.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def start_operation(self, operation: str) -> None:
        """
        Start a new operation.
        
        Args:
            operation: Operation description
        """
        self.status = "Running"
        self.current_operation = operation
        self.progress = 0.0
        self.start_time = time.time()
        self.end_time = 0.0
        
        # Update UI elements
        status_text = self.query_one("#status-text", Static)
        operation_text = self.query_one("#operation-text", Static)
        progress_bar = self.query_one("#progress-bar", ProgressBar)
        elapsed_time = self.query_one("#elapsed-time", Static)
        estimated_time = self.query_one("#estimated-time", Static)
        
        status_text.update("Running")
        operation_text.update(operation)
        progress_bar.progress = 0.0
        elapsed_time.update("00:00:00")
        estimated_time.update("--:--:--")
        
        # Log the operation start
        self.log(f"[bold green]Started:[/bold green] {operation}")
    
    def update_progress(self, progress: float, message: Optional[str] = None) -> None:
        """
        Update the progress of the current operation.
        
        Args:
            progress: Progress value (0.0 to 1.0)
            message: Optional progress message
        """
        self.progress = max(0.0, min(1.0, progress))
        
        # Update progress bar
        progress_bar = self.query_one("#progress-bar", ProgressBar)
        progress_bar.progress = self.progress
        
        # Log the progress message if provided
        if message:
            self.log(f"[blue]Progress ({int(self.progress * 100)}%):[/blue] {message}")
    
    def complete_operation(self, success: bool = True, message: Optional[str] = None) -> None:
        """
        Complete the current operation.
        
        Args:
            success: Whether the operation was successful
            message: Optional completion message
        """
        self.status = "Success" if success else "Failed"
        self.progress = 1.0 if success else self.progress
        self.end_time = time.time()
        
        # Update UI elements
        status_text = self.query_one("#status-text", Static)
        progress_bar = self.query_one("#progress-bar", ProgressBar)
        
        status_text.update("[green]Success[/green]" if success else "[red]Failed[/red]")
        
        if success:
            progress_bar.progress = 1.0
        
        # Log the operation completion
        if success:
            self.log(f"[bold green]Completed:[/bold green] {self.current_operation}")
            if message:
                self.log(f"[green]{message}[/green]")
        else:
            self.log(f"[bold red]Failed:[/bold red] {self.current_operation}")
            if message:
                self.log(f"[red]{message}[/red]")
    
    def log(self, message: str) -> None:
        """
        Add a message to the log.
        
        Args:
            message: Log message
        """
        log = self.query_one("#progress-log", Log)
        log.write(message)
    
    def clear_log(self) -> None:
        """Clear the log."""
        log = self.query_one("#progress-log", Log)
        log.clear()
    
    def reset(self) -> None:
        """Reset the progress display."""
        self.status = "Idle"
        self.current_operation = ""
        self.progress = 0.0
        self.start_time = 0.0
        self.end_time = 0.0
        
        # Update UI elements
        status_text = self.query_one("#status-text", Static)
        operation_text = self.query_one("#operation-text", Static)
        progress_bar = self.query_one("#progress-bar", ProgressBar)
        elapsed_time = self.query_one("#elapsed-time", Static)
        estimated_time = self.query_one("#estimated-time", Static)
        
        status_text.update("Idle")
        operation_text.update("")
        progress_bar.progress = 0.0
        elapsed_time.update("00:00:00")
        estimated_time.update("--:--:--")
        
        # Clear the log
        self.clear_log()