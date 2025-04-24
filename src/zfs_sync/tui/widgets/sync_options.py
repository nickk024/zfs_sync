"""
Sync Options Widget

This module provides a widget for configuring ZFS synchronization options.
"""

import logging
from typing import Callable, Optional, Dict, Any, List

from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.widgets import Switch, Select, Label, Button
from textual.reactive import reactive

logger = logging.getLogger('zfs_sync.tui.widgets.sync_options')

class SyncOptions(Vertical):
    """Widget for configuring ZFS synchronization options."""
    
    recursive = reactive(True)
    compress = reactive("lz4")
    create_bookmark = reactive(True)
    preserve_properties = reactive(True)
    no_stream = reactive(False)
    
    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        on_options_change: Optional[Callable[[Dict[str, Any]], None]] = None,
        initial_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the sync options widget.
        
        Args:
            id: Widget ID
            name: Widget name
            on_options_change: Callback function to call when options change
            initial_options: Initial options
        """
        super().__init__(id=id, name=name)
        self.on_options_change_callback = on_options_change
        self.initial_options = initial_options or {}
    
    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        with Horizontal(classes="sync-option-row"):
            yield Label("Recursive:", classes="sync-option-label")
            yield Switch(
                value=self.initial_options.get("recursive", True),
                id="recursive-switch",
            )
        
        with Horizontal(classes="sync-option-row"):
            yield Label("Compression:", classes="sync-option-label")
            yield Select(
                options=[
                    ("None", "none"),
                    ("LZ4 (Default)", "lz4"),
                    ("GZIP", "gzip"),
                    ("PIGZ (Fast)", "pigz-fast"),
                    ("PIGZ (Slow)", "pigz-slow"),
                    ("ZSTD (Fast)", "zstd-fast"),
                    ("ZSTD (Slow)", "zstd-slow"),
                    ("XZ", "xz"),
                    ("LZO", "lzo"),
                ],
                value=self.initial_options.get("compress", "lz4"),
                id="compress-select",
            )
        
        with Horizontal(classes="sync-option-row"):
            yield Label("Create Bookmark:", classes="sync-option-label")
            yield Switch(
                value=self.initial_options.get("create_bookmark", True),
                id="create-bookmark-switch",
            )
        
        with Horizontal(classes="sync-option-row"):
            yield Label("Preserve Properties:", classes="sync-option-label")
            yield Switch(
                value=self.initial_options.get("preserve_properties", True),
                id="preserve-properties-switch",
            )
        
        with Horizontal(classes="sync-option-row"):
            yield Label("No Stream:", classes="sync-option-label")
            yield Switch(
                value=self.initial_options.get("no_stream", False),
                id="no-stream-switch",
            )
        
        with Horizontal(classes="sync-option-row"):
            yield Label("Advanced Options:", classes="sync-option-label")
            yield Button("Configure", id="advanced-options-button", variant="primary")
        
        with Horizontal(classes="sync-option-buttons"):
            yield Button("Reset to Defaults", id="reset-options-button", variant="default")
            yield Button("Save", id="save-options-button", variant="success")
    
    def on_mount(self) -> None:
        """Called when the widget is mounted."""
        # Set initial values from initial_options
        self.recursive = self.initial_options.get("recursive", True)
        self.compress = self.initial_options.get("compress", "lz4")
        self.create_bookmark = self.initial_options.get("create_bookmark", True)
        self.preserve_properties = self.initial_options.get("preserve_properties", True)
        self.no_stream = self.initial_options.get("no_stream", False)
    
    def on_switch_changed(self, event) -> None:
        """Called when a switch value changes."""
        switch_id = event.switch.id
        
        if switch_id == "recursive-switch":
            self.recursive = event.value
        elif switch_id == "create-bookmark-switch":
            self.create_bookmark = event.value
        elif switch_id == "preserve-properties-switch":
            self.preserve_properties = event.value
        elif switch_id == "no-stream-switch":
            self.no_stream = event.value
        
        # Notify about options change
        if self.on_options_change_callback:
            self.on_options_change_callback(self.get_options())
    
    def on_select_changed(self, event) -> None:
        """Called when a select value changes."""
        select_id = event.select.id
        
        if select_id == "compress-select":
            self.compress = event.value
        
        # Notify about options change
        if self.on_options_change_callback:
            self.on_options_change_callback(self.get_options())
    
    def on_button_pressed(self, event) -> None:
        """Called when a button is pressed."""
        button_id = event.button.id
        
        if button_id == "advanced-options-button":
            self.show_advanced_options()
        elif button_id == "reset-options-button":
            self.reset_to_defaults()
        elif button_id == "save-options-button":
            self.save_options()
    
    def show_advanced_options(self) -> None:
        """Show advanced options dialog."""
        # This will be implemented later with a modal dialog
        self.app.notify("Advanced options not yet implemented")
    
    def reset_to_defaults(self) -> None:
        """Reset options to defaults."""
        self.recursive = True
        self.compress = "lz4"
        self.create_bookmark = True
        self.preserve_properties = True
        self.no_stream = False
        
        # Update UI elements
        self.query_one("#recursive-switch", Switch).value = self.recursive
        self.query_one("#compress-select", Select).value = self.compress
        self.query_one("#create-bookmark-switch", Switch).value = self.create_bookmark
        self.query_one("#preserve-properties-switch", Switch).value = self.preserve_properties
        self.query_one("#no-stream-switch", Switch).value = self.no_stream
        
        # Notify about options change
        if self.on_options_change_callback:
            self.on_options_change_callback(self.get_options())
        
        self.app.notify("Options reset to defaults")
    
    def save_options(self) -> None:
        """Save the sync options."""
        options = self.get_options()
        
        if self.on_options_change_callback:
            self.on_options_change_callback(options)
        
        self.app.notify("Sync options saved")
    
    def get_options(self) -> Dict[str, Any]:
        """
        Get the current sync options.
        
        Returns:
            Sync options dictionary
        """
        return {
            "recursive": self.recursive,
            "compress": self.compress,
            "create-bookmark": self.create_bookmark,
            "preserve-properties": self.preserve_properties,
            "no-stream": self.no_stream,
        }
    
    def set_options(self, options: Dict[str, Any]) -> None:
        """
        Set the sync options.
        
        Args:
            options: Sync options dictionary
        """
        self.recursive = options.get("recursive", True)
        self.compress = options.get("compress", "lz4")
        self.create_bookmark = options.get("create-bookmark", True)
        self.preserve_properties = options.get("preserve-properties", True)
        self.no_stream = options.get("no-stream", False)
        
        # Update UI elements
        self.query_one("#recursive-switch", Switch).value = self.recursive
        self.query_one("#compress-select", Select).value = self.compress
        self.query_one("#create-bookmark-switch", Switch).value = self.create_bookmark
        self.query_one("#preserve-properties-switch", Switch).value = self.preserve_properties
        self.query_one("#no-stream-switch", Switch).value = self.no_stream