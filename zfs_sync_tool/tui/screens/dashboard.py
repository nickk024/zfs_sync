from textual.screen import Screen
from textual.app import ComposeResult
from textual.widgets import Header, Footer, Static
from textual.containers import Container
import logging

# Import the screen we want to navigate to
from .dataset_select import DatasetSelectScreen
# Import the core app type hint for clarity (optional but good practice)
# This might require adjusting imports if circular dependencies arise,
# potentially using TYPE_CHECKING guard. For now, assume direct import works or skip.
# from ...core.app import ZfsSyncApp

logger = logging.getLogger(__name__)

class DashboardScreen(Screen):
    """The main dashboard screen displaying system overview."""

    BINDINGS = [
        ("s", "show_datasets", "Select Datasets"),
        # Add other screen-specific bindings if needed
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the dashboard screen."""
        yield Container(
            Static("Dashboard - System Overview (Placeholder)\n\nPress 's' to select datasets.", id="dashboard-content")
            # Add more widgets here later (e.g., status panels, task lists)
        )
        logger.debug("DashboardScreen composed.")

    def on_mount(self) -> None:
        """Called when the screen is mounted."""
        logger.info("DashboardScreen mounted.")
        # You can load initial data here

    def action_show_datasets(self) -> None:
        """Called when the user presses 's'."""
        logger.info("Action 'show_datasets' triggered.")
        try:
            # Access the core app logic via the TUI app instance (self.app)
            # core_app: ZfsSyncApp = self.app.core_app # Type hint for clarity
            datasets = self.app.core_app.zfs.list_datasets("filesystem")
            logger.debug(f"Fetched {len(datasets)} datasets.")
            # Create and push the dataset selection screen, passing the data
            dataset_screen = DatasetSelectScreen(datasets=datasets)
            self.app.push_screen(dataset_screen)
        except AttributeError as e:
             logger.error(f"Could not access core app or ZFS interface: {e}. Is the app structure correct?", exc_info=True)
             # Optionally show an error message to the user on the TUI
        except Exception as e:
            logger.error(f"Failed to fetch datasets or push screen: {e}", exc_info=True)
            # Optionally show an error message to the user on the TUI