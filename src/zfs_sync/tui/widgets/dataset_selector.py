"""
Dataset Selector Widget

This module provides a widget for selecting ZFS datasets.
"""

import logging
from typing import List, Callable, Optional

from textual.widgets import Select, SelectionList, SelectionType
from textual.widgets.selection_list import Selection
from textual.reactive import reactive

from zfs_sync.core.zfs_ops import list_datasets

logger = logging.getLogger('zfs_sync.tui.widgets.dataset_selector')

class DatasetSelector(SelectionList):
    """Widget for selecting ZFS datasets."""
    
    BINDINGS = [
        ("r", "refresh", "Refresh"),
    ]
    
    datasets = reactive([])
    selected_dataset = reactive("")
    
    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        on_select: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Initialize the dataset selector.
        
        Args:
            id: Widget ID
            name: Widget name
            on_select: Callback function to call when a dataset is selected
        """
        super().__init__([], id=id, name=name)
        self.on_select_callback = on_select
    
    def on_mount(self) -> None:
        """Called when the widget is mounted."""
        self.refresh_datasets()
    
    def refresh_datasets(self) -> None:
        """Refresh the list of datasets."""
        try:
            self.datasets = list_datasets()
            
            # Convert datasets to Selection objects
            selections = [
                Selection(dataset, dataset)
                for dataset in self.datasets
            ]
            
            # Update the selection list
            self.clear_options()
            self.add_options(selections)
            
            logger.debug(f"Refreshed datasets: {len(self.datasets)} found")
        except Exception as e:
            logger.error(f"Failed to refresh datasets: {e}")
            self.app.notify(f"Failed to refresh datasets: {e}", severity="error")
    
    def action_refresh(self) -> None:
        """Refresh the list of datasets."""
        self.refresh_datasets()
    
    def on_selection_list_selected(self, event) -> None:
        """Called when a dataset is selected."""
        self.selected_dataset = event.selection.value
        
        if self.on_select_callback:
            self.on_select_callback(self.selected_dataset)