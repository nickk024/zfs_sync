from textual.screen import Screen
from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable, Static, Label
from textual.containers import Container, VerticalScroll
import logging
from typing import List

# Assuming core_app provides access to zfs interface
# from ...core.app import ZfsSyncApp # Would cause circular import if App imports Screen directly

logger = logging.getLogger(__name__)

class DatasetSelectScreen(Screen):
    """A screen for selecting ZFS datasets."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        # Add bindings for selection, refresh, etc.
    ]

    def __init__(self, datasets: List[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datasets = datasets if datasets is not None else []
        logger.debug(f"DatasetSelectScreen initialized with {len(self._datasets)} datasets.")

    def compose(self) -> ComposeResult:
        """Create child widgets for the dataset selection screen."""
        yield Header()
        yield Container(
            Label("Available ZFS Filesystems:"),
            VerticalScroll(
                DataTable(id="dataset-table", cursor_type="row", zebra_stripes=True)
            )
        )
        yield Footer()
        logger.debug("DatasetSelectScreen composed.")

    def on_mount(self) -> None:
        """Called when the screen is mounted. Load data into the table."""
        logger.info("DatasetSelectScreen mounted.")
        table = self.query_one(DataTable)
        table.add_column("Filesystem Name", key="name")

        if not self._datasets:
             logger.warning("No datasets provided to DatasetSelectScreen.")
             # Optionally, try to load them here if not passed in init
             # try:
             #     # Access core_app via self.app (Textual convention)
             #     core_app: ZfsSyncApp = self.app
             #     self._datasets = core_app.zfs.list_datasets("filesystem")
             #     logger.info(f"Loaded {len(self._datasets)} datasets dynamically.")
             # except Exception as e:
             #     logger.error(f"Failed to load datasets dynamically: {e}", exc_info=True)
             #     self.query_one("#dataset-table").display = False # Hide table
             #     self.mount(Static("Error loading datasets.", classes="error-message")) # Show error

        if self._datasets:
            logger.debug(f"Populating DataTable with {len(self._datasets)} datasets.")
            for i, dataset_name in enumerate(self._datasets):
                table.add_row(dataset_name, key=str(i))
        else:
             # Handle case where no datasets are found/loaded
             table.add_row("No filesystems found.", key="none")
             logger.info("DataTable populated with 'No filesystems found'.")

    # Add event handlers for table selection (e.g., on_data_table_row_selected)
    # def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
    #     row_key = event.row_key.value
    #     if row_key is not None and row_key != "none":
    #         selected_dataset = self._datasets[int(row_key)]
    #         logger.info(f"Dataset selected: {selected_dataset}")
    #         # TODO: Do something with the selection (e.g., show details, pass back to previous screen)
    #         # self.dismiss(selected_dataset) # Example: close screen and return value