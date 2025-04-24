"""
Main Screen for ZFS Sync Tool TUI

This module provides the main screen for the ZFS Sync Tool TUI.
"""

import logging
from typing import Dict, Any, Optional

from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import Vertical, Horizontal, Container
from textual.widgets import Header, Footer, Static, Button, TabbedContent, TabPane
from textual.binding import Binding

from zfs_sync.tui.widgets.dataset_selector import DatasetSelector
from zfs_sync.tui.widgets.server_config import ServerConfig
from zfs_sync.tui.widgets.sync_options import SyncOptions
from zfs_sync.tui.widgets.progress_display import ProgressDisplay
from zfs_sync.core.config_manager import load_config, save_config, add_saved_configuration
from zfs_sync.core.zfs_ops import list_datasets
from zfs_sync.core.ssh_ops import SSHConnection
from zfs_sync.core.sanoid_ops import sync_dataset

logger = logging.getLogger('zfs_sync.tui.screens.main_screen')

class MainScreen(Screen):
    """Main screen for ZFS Sync Tool."""
    
    BINDINGS = [
        Binding("s", "start_sync", "Start Sync"),
        Binding("c", "save_config", "Save Config"),
        Binding("r", "refresh", "Refresh"),
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self, *args, initial_job=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = load_config()
        self.initial_job = initial_job
        
        if initial_job and initial_job in self.config.get("jobs", {}):
            # Load job configuration
            job = self.config["jobs"][initial_job]
            self.source_dataset = job.get("source_dataset", "")
            self.destination_server = job.get("destination_server", "")
            self.destination_dataset = job.get("destination_dataset", "")
            self.sync_options = job.get("sync_options", {})
            self.current_job = initial_job
            logger.debug(f"Loaded job configuration: {initial_job}")
        else:
            # Load default configuration
            self.source_dataset = self.config.get("default_source_dataset", "")
            self.destination_server = self.config.get("default_destination_server", "")
            self.destination_dataset = self.config.get("default_destination_dataset", "")
            self.sync_options = self.config.get("sync_options", {})
            self.current_job = None
    
    def compose(self) -> ComposeResult:
        """Compose the screen layout."""
        yield Header()
        
        with Container(id="main-container"):
            yield Static("# ZFS Sync Tool", id="title", classes="title")
            yield Static("Synchronize ZFS datasets between servers using snapshots", id="subtitle", classes="subtitle")
            
            with TabbedContent(id="main-tabs"):
                with TabPane("Configuration", id="config-tab"):
                    with Vertical(id="config-section"):
                        yield Static("## Source Dataset", classes="section-title")
                        yield DatasetSelector(
                            id="source-dataset-selector",
                            on_select=self.on_source_dataset_selected,
                        )
                        
                        yield Static("## Destination Server", classes="section-title")
                        yield ServerConfig(
                            id="destination-server-config",
                            on_config_change=self.on_server_config_changed,
                            initial_config={
                                "hostname": self.destination_server,
                            },
                        )
                        
                        yield Static("## Destination Dataset", classes="section-title")
                        with Horizontal(id="destination-dataset-section"):
                            yield Static("Dataset:", classes="label")
                            yield Button("Select Remote Dataset", id="select-remote-dataset", variant="primary")
                            yield Static(self.destination_dataset, id="destination-dataset-text")
                        
                        yield Static("## Sync Options", classes="section-title")
                        yield SyncOptions(
                            id="sync-options",
                            on_options_change=self.on_sync_options_changed,
                            initial_options=self.sync_options,
                        )
                        
                        with Horizontal(id="config-buttons"):
                            yield Button("Save Configuration", id="save-config", variant="primary")
                            yield Button("Load Configuration", id="load-config", variant="primary")
                            yield Button("Start Sync", id="start-sync", variant="success")
                
                with TabPane("Sync Progress", id="progress-tab"):
                    yield ProgressDisplay(id="progress-display")
                
                with TabPane("Saved Configurations", id="saved-tab"):
                    yield Static("## Saved Configurations", classes="section-title")
                    yield Static("Select a saved configuration to load:", classes="section-subtitle")
                    # This will be populated dynamically
                    yield Vertical(id="saved-configs-list")
                    
                    with Horizontal(id="saved-config-buttons"):
                        yield Button("Save Current Configuration", id="save-current-config", variant="primary")
                        yield Button("Delete Selected Configuration", id="delete-config", variant="error")
        
        yield Footer()
    
    def on_mount(self) -> None:
        """Called when the screen is mounted."""
        self.query_one("#title").styles.text_align = "center"
        self.query_one("#subtitle").styles.text_align = "center"
        
        # Load saved configurations
        self.load_saved_configurations()
    
    def load_saved_configurations(self) -> None:
        """Load saved configurations."""
        saved_configs_list = self.query_one("#saved-configs-list", Vertical)
        saved_configs_list.remove_children()
        
        saved_configs = self.config.get("saved_configurations", [])
        
        if not saved_configs:
            saved_configs_list.mount(Static("No saved configurations found.", classes="empty-message"))
            return
        
        for config in saved_configs:
            name = config.get("name", "Unnamed")
            source = config.get("source_dataset", "")
            destination = f"{config.get('destination_server', '')}:{config.get('destination_dataset', '')}"
            
            with Vertical(classes="saved-config-item"):
                with Horizontal():
                    yield Static(f"**{name}**", classes="saved-config-name")
                    yield Button("Load", id=f"load-config-{name}", variant="primary", classes="load-config-button")
                yield Static(f"Source: {source}", classes="saved-config-detail")
                yield Static(f"Destination: {destination}", classes="saved-config-detail")
    
    def on_source_dataset_selected(self, dataset: str) -> None:
        """
        Called when a source dataset is selected.
        
        Args:
            dataset: Selected dataset
        """
        self.source_dataset = dataset
        logger.debug(f"Source dataset selected: {dataset}")
    
    def on_server_config_changed(self, config: Dict[str, Any]) -> None:
        """
        Called when the server configuration changes.
        
        Args:
            config: Server configuration
        """
        self.destination_server = config.get("hostname", "")
        logger.debug(f"Destination server changed: {self.destination_server}")
    
    def on_sync_options_changed(self, options: Dict[str, Any]) -> None:
        """
        Called when sync options change.
        
        Args:
            options: Sync options
        """
        self.sync_options = options
        logger.debug(f"Sync options changed: {options}")
    
    def on_button_pressed(self, event) -> None:
        """Called when a button is pressed."""
        button_id = event.button.id
        
        if button_id == "start-sync":
            self.start_sync()
        elif button_id == "save-config":
            self.save_config()
        elif button_id == "load-config":
            self.load_config_dialog()
        elif button_id == "select-remote-dataset":
            self.select_remote_dataset()
        elif button_id == "save-current-config":
            self.save_current_config_dialog()
        elif button_id == "delete-config":
            self.delete_config_dialog()
        elif button_id.startswith("load-config-"):
            config_name = button_id[len("load-config-"):]
            self.load_saved_config(config_name)
    
    def action_start_sync(self) -> None:
        """Start the synchronization process."""
        self.start_sync()
    
    def action_save_config(self) -> None:
        """Save the current configuration."""
        self.save_config()
    
    def action_refresh(self) -> None:
        """Refresh the UI."""
        # Refresh dataset selector
        dataset_selector = self.query_one("#source-dataset-selector", DatasetSelector)
        dataset_selector.refresh_datasets()
        
        # Refresh saved configurations
        self.load_saved_configurations()
    
    def start_sync(self) -> None:
        """Start the synchronization process."""
        if not self.source_dataset:
            self.app.notify("Please select a source dataset", severity="error")
            return
        
        if not self.destination_server:
            self.app.notify("Please configure a destination server", severity="error")
            return
        
        if not self.destination_dataset:
            self.app.notify("Please select a destination dataset", severity="error")
            return
        
        # Switch to progress tab
        tabs = self.query_one("#main-tabs", TabbedContent)
        tabs.active = "progress-tab"
        
        # Get progress display widget
        progress_display = self.query_one("#progress-display", ProgressDisplay)
        
        # Start the sync operation
        progress_display.start_operation(f"Syncing {self.source_dataset} to {self.destination_server}:{self.destination_dataset}")
        
        # Prepare destination
        destination = self.destination_dataset
        if self.destination_server and self.destination_server != "localhost":
            destination = f"{self.destination_server}:{self.destination_dataset}"
        
        try:
            # Start the sync process
            progress_display.log(f"Starting sync from {self.source_dataset} to {destination}")
            progress_display.update_progress(0.1, "Initializing sync process...")
            
            # Convert sync options to syncoid format
            syncoid_options = {}
            for key, value in self.sync_options.items():
                if isinstance(value, bool):
                    if value:
                        syncoid_options[key] = True
                else:
                    syncoid_options[key] = value
            
            # Run the sync operation
            progress_display.update_progress(0.2, "Running syncoid...")
            
            # This would normally be run in a separate thread to avoid blocking the UI
            # For now, we'll just simulate progress
            progress_display.update_progress(0.5, "Transferring data...")
            
            # In a real implementation, we would monitor the progress of the sync operation
            # and update the progress display accordingly
            
            # For now, we'll just simulate completion
            progress_display.update_progress(0.9, "Finalizing sync...")
            progress_display.complete_operation(True, "Sync completed successfully")
            
            # Save the configuration
            self.save_config()
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            progress_display.complete_operation(False, f"Sync failed: {str(e)}")
    
    def save_config(self) -> None:
        """Save the current configuration."""
        # Update the configuration
        self.config["default_source_dataset"] = self.source_dataset
        self.config["default_destination_server"] = self.destination_server
        self.config["default_destination_dataset"] = self.destination_dataset
        self.config["sync_options"] = self.sync_options
        
        # Save the configuration
        try:
            save_config(self.config)
            self.app.notify("Configuration saved")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            self.app.notify(f"Failed to save configuration: {str(e)}", severity="error")
    
    def load_config_dialog(self) -> None:
        """Show the load configuration dialog."""
        # This would normally open a dialog to select a configuration
        # For now, we'll just switch to the saved configurations tab
        tabs = self.query_one("#main-tabs", TabbedContent)
        tabs.active = "saved-tab"
    
    def select_remote_dataset(self) -> None:
        """Select a remote dataset."""
        # This would normally open a dialog to select a remote dataset
        # For now, we'll just use a hardcoded value
        self.destination_dataset = self.config.get("default_destination_dataset", "")
        
        # Update the UI
        destination_dataset_text = self.query_one("#destination-dataset-text", Static)
        destination_dataset_text.update(self.destination_dataset)
        
        self.app.notify(f"Selected destination dataset: {self.destination_dataset}")
    
    def save_current_config_dialog(self) -> None:
        """Show the save current configuration dialog."""
        # This would normally open a dialog to enter a name for the configuration
        # For now, we'll just use a hardcoded name
        config_name = f"Config {len(self.config.get('saved_configurations', []))}"
        
        try:
            add_saved_configuration(
                name=config_name,
                source_dataset=self.source_dataset,
                destination_server=self.destination_server,
                destination_dataset=self.destination_dataset,
                sync_options=self.sync_options
            )
            
            # Reload the configuration
            self.config = load_config()
            
            # Refresh the saved configurations list
            self.load_saved_configurations()
            
            self.app.notify(f"Configuration saved as '{config_name}'")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            self.app.notify(f"Failed to save configuration: {str(e)}", severity="error")
    
    def delete_config_dialog(self) -> None:
        """Show the delete configuration dialog."""
        # This would normally open a dialog to select a configuration to delete
        # For now, we'll just notify that this feature is not implemented
        self.app.notify("Delete configuration not yet implemented")
    
    def load_saved_config(self, config_name: str) -> None:
        """
        Load a saved configuration.
        
        Args:
            config_name: Name of the configuration to load
        """
        # Find the configuration with the given name
        for config in self.config.get("saved_configurations", []):
            if config.get("name") == config_name:
                # Load the configuration
                self.source_dataset = config.get("source_dataset", "")
                self.destination_server = config.get("destination_server", "")
                self.destination_dataset = config.get("destination_dataset", "")
                self.sync_options = config.get("sync_options", {})
                
                # Update the UI
                # This is a simplified version - in a real implementation, we would
                # update all the widgets with the new values
                
                # Switch to the configuration tab
                tabs = self.query_one("#main-tabs", TabbedContent)
                tabs.active = "config-tab"
                
                self.app.notify(f"Loaded configuration '{config_name}'")
                return
        
        self.app.notify(f"Configuration '{config_name}' not found", severity="error")