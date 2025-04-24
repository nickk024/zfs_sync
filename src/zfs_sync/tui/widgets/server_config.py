"""
Server Configuration Widget

This module provides a widget for configuring remote servers.
"""

import logging
from typing import Callable, Optional, Dict, Any

from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.widgets import Input, Button, Label, Static
from textual.reactive import reactive

from zfs_sync.core.ssh_ops import test_ssh_connection, get_known_hosts

logger = logging.getLogger('zfs_sync.tui.widgets.server_config')

class ServerConfig(Vertical):
    """Widget for configuring remote servers."""
    
    BINDINGS = [
        ("t", "test_connection", "Test Connection"),
    ]
    
    hostname = reactive("")
    username = reactive("")
    port = reactive(22)
    key_file = reactive("")
    
    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        on_config_change: Optional[Callable[[Dict[str, Any]], None]] = None,
        initial_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the server configuration widget.
        
        Args:
            id: Widget ID
            name: Widget name
            on_config_change: Callback function to call when the configuration changes
            initial_config: Initial configuration
        """
        super().__init__(id=id, name=name)
        self.on_config_change_callback = on_config_change
        self.initial_config = initial_config or {}
    
    def compose(self) -> ComposeResult:
        """Compose the widget layout."""
        with Horizontal(classes="server-config-row"):
            yield Label("Hostname:", classes="server-config-label")
            yield Input(
                placeholder="Enter hostname or IP",
                id="hostname-input",
                value=self.initial_config.get("hostname", ""),
            )
        
        with Horizontal(classes="server-config-row"):
            yield Label("Username:", classes="server-config-label")
            yield Input(
                placeholder="Enter username (optional)",
                id="username-input",
                value=self.initial_config.get("username", ""),
            )
        
        with Horizontal(classes="server-config-row"):
            yield Label("Port:", classes="server-config-label")
            yield Input(
                placeholder="22",
                id="port-input",
                value=str(self.initial_config.get("port", 22)),
            )
        
        with Horizontal(classes="server-config-row"):
            yield Label("SSH Key:", classes="server-config-label")
            yield Input(
                placeholder="Path to SSH key (optional)",
                id="key-file-input",
                value=self.initial_config.get("key_file", ""),
            )
        
        with Horizontal(classes="server-config-buttons"):
            yield Button("Test Connection", id="test-connection-button", variant="primary")
            yield Button("Save", id="save-config-button", variant="success")
        
        yield Static(id="connection-status", classes="connection-status")
    
    def on_mount(self) -> None:
        """Called when the widget is mounted."""
        # Load known hosts for autocomplete
        self.load_known_hosts()
    
    def load_known_hosts(self) -> None:
        """Load known hosts for autocomplete."""
        try:
            known_hosts = get_known_hosts()
            hostname_input = self.query_one("#hostname-input", Input)
            hostname_input.suggestions = known_hosts
        except Exception as e:
            logger.error(f"Failed to load known hosts: {e}")
    
    def on_input_changed(self, event) -> None:
        """Called when an input value changes."""
        input_id = event.input.id
        
        if input_id == "hostname-input":
            self.hostname = event.value
        elif input_id == "username-input":
            self.username = event.value
        elif input_id == "port-input":
            try:
                self.port = int(event.value) if event.value else 22
            except ValueError:
                self.port = 22
                event.input.value = "22"
        elif input_id == "key-file-input":
            self.key_file = event.value
        
        # Notify about configuration change
        if self.on_config_change_callback:
            self.on_config_change_callback(self.get_config())
    
    def on_button_pressed(self, event) -> None:
        """Called when a button is pressed."""
        button_id = event.button.id
        
        if button_id == "test-connection-button":
            self.test_connection()
        elif button_id == "save-config-button":
            self.save_config()
    
    def action_test_connection(self) -> None:
        """Test the SSH connection."""
        self.test_connection()
    
    def test_connection(self) -> None:
        """Test the SSH connection."""
        status = self.query_one("#connection-status", Static)
        status.update("Testing connection...")
        
        config = self.get_config()
        
        if not config["hostname"]:
            status.update("[red]Error: Hostname is required[/red]")
            return
        
        try:
            result = test_ssh_connection(
                hostname=config["hostname"],
                username=config["username"] if config["username"] else None,
                port=config["port"],
                key_filename=config["key_file"] if config["key_file"] else None,
            )
            
            if result:
                status.update("[green]Connection successful![/green]")
            else:
                status.update("[red]Connection failed![/red]")
        except Exception as e:
            logger.error(f"Failed to test connection: {e}")
            status.update(f"[red]Error: {str(e)}[/red]")
    
    def save_config(self) -> None:
        """Save the server configuration."""
        config = self.get_config()
        
        if not config["hostname"]:
            self.app.notify("Hostname is required", severity="error")
            return
        
        if self.on_config_change_callback:
            self.on_config_change_callback(config)
        
        self.app.notify("Server configuration saved")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the current server configuration.
        
        Returns:
            Server configuration dictionary
        """
        return {
            "hostname": self.hostname,
            "username": self.username,
            "port": self.port,
            "key_file": self.key_file,
        }
    
    def set_config(self, config: Dict[str, Any]) -> None:
        """
        Set the server configuration.
        
        Args:
            config: Server configuration dictionary
        """
        self.hostname = config.get("hostname", "")
        self.username = config.get("username", "")
        self.port = config.get("port", 22)
        self.key_file = config.get("key_file", "")
        
        # Update input values
        hostname_input = self.query_one("#hostname-input", Input)
        username_input = self.query_one("#username-input", Input)
        port_input = self.query_one("#port-input", Input)
        key_file_input = self.query_one("#key-file-input", Input)
        
        hostname_input.value = self.hostname
        username_input.value = self.username
        port_input.value = str(self.port)
        key_file_input.value = self.key_file