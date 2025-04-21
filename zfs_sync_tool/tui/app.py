from textual.app import App, ComposeResult
from textual.widgets import Header, Footer
# Removed Static and Container import as they are no longer directly used here
import logging
import sys # Import sys module

# Import the screen
from .screens.dashboard import DashboardScreen

logger = logging.getLogger(__name__)

class ZfsSyncTuiApp(App):
    """The main Textual TUI application for ZFS Sync Tool."""

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
        # Add other global bindings here (e.g., for navigation)
    ]

    # CSS files are loaded relative to the App class
    CSS_PATH = "styles.css"

    def __init__(self, core_app, *args, **kwargs):
        """
        Initialize the TUI App.

        Args:
            core_app: An instance of the core ZfsSyncApp logic.
        """
        super().__init__(*args, **kwargs)
        self.core_app = core_app
        logger.info("TUI Application initialized.")

    def compose(self) -> ComposeResult:
        """Create child widgets for the app. Header and Footer are composed automatically."""
        # The main content area will be managed by the Screen stack.
        # We yield Header and Footer, Textual places them correctly.
        yield Header()
        yield Footer()
        # No need to yield a container here, screens handle their own content.

    def on_mount(self) -> None:
        """Called after the app is mounted."""
        # Push the initial screen onto the stack
        logger.debug("App mounted, pushing DashboardScreen.")
        self.push_screen(DashboardScreen())

    def action_toggle_dark(self) -> None:
        """Called when the user presses 'd' to toggle dark mode."""
        self.dark = not self.dark
        logger.debug(f"Dark mode toggled: {self.dark}")

    def action_quit(self) -> None:
        """Called when the user presses 'q' to quit."""
        logger.info("Quit action triggered.")
        self.exit("User requested quit.") # Pass optional result
        sys.exit(0) # Ensure the process exits cleanly


# Example of how to run this TUI (will be integrated into core/app.py later)
if __name__ == "__main__":
    # For standalone testing, create a dummy core_app
    class DummyCoreApp:
        def __init__(self):
            self.config = {"general": {"log_level": "DEBUG"}} # Dummy config for logging
            # Add other dummy attributes/methods if needed by TUI during testing

        def _configure_logging(self): # Need this method for TUI init
             logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    dummy_app_logic = DummyCoreApp()
    dummy_app_logic._configure_logging() # Configure logging for the dummy run

    tui_app = ZfsSyncTuiApp(core_app=dummy_app_logic)
    tui_app.run()