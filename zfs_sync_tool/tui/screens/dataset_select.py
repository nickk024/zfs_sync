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
        # Add bindings for selection (Enter is handled by DataTable message), refresh, etc.
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
            # Use add_rows for potentially better performance with many rows
            rows = [(ds,) for ds in self._datasets] # DataTable expects tuples for rows
            table.add_rows(rows)
            # Add keys after rows are added if needed, or generate keys differently
            # For now, relying on implicit row index or potentially adding key in add_rows if supported
        else:
             # Handle case where no datasets are found/loaded
             table.add_row("No filesystems found.", key="none")
             logger.info("DataTable populated with 'No filesystems found'.")

        # Set focus to the table after populating it
        table.focus()
        logger.debug("Focus set on DataTable.")

    # Add event handlers for table selection (e.g., on_data_table_row_selected)
    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle the row selection event when Enter is pressed."""
        # DataTable sends this message when a row is selected (usually by Enter)
        table = self.query_one(DataTable)
        # Get the data for the selected row; event.row_key might not be reliable if keys weren't added correctly
        # Instead, get data directly using the cursor coordinate
        try:
            row_data = table.get_row_at(event.cursor_row)
            # Assuming the first column is the dataset name
            selected_dataset = row_data[0]
            if selected_dataset != "No filesystems found.":
                 logger.info(f"Dataset selected via Enter: {selected_dataset}")
                 # TODO: Do something with the selection (e.g., show details, pass back)
                 # Example: Show a notification (requires importing Notification)
                 # self.app.notify(f"Selected: {selected_dataset}")
                 # Example: Pop screen and return value
                 # self.dismiss(selected_dataset)
            else:
                 logger.debug("Enter pressed on 'No filesystems found.' row.")
        except IndexError:
             logger.error(f"Could not get row data for cursor row {event.cursor_row}")
        except Exception as e:
             logger.error(f"Error handling row selection: {e}", exc_info=True)