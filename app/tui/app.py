import logging
import json
from pathlib import Path
from typing import Optional, Dict, Any

from textual.app import App, CSSPathType
from textual.binding import Binding

# Import screens and messages
from .screens.host_ssh import HostSSHScreen
from .screens.transfer import TransferScreen
from .messages import StartTransfer
from .constants import STATE_FILE_PATH # Import from constants

# --- Utility Functions (TUI specific) ---

def _load_interactive_state(state_file: Path) -> Dict[str, Any]:
    """Loads the last interactive state from a JSON file."""
    if state_file.is_file():
        try:
            with state_file.open('r') as f:
                state = json.load(f)
                logging.debug(f"Loaded interactive state from {state_file}: {state}")
                return state if isinstance(state, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Could not load or parse state file {state_file}: {e}")
            return {}
    else:
        logging.debug(f"Interactive state file {state_file} not found.")
        return {}

# _save_interactive_state was moved to host_ssh.py to resolve circular import.

# --- Main App ---

class ZFSSyncApp(App[Optional[Dict]]): # Return job config on exit?
    """The main Textual application for zfs-sync."""

    # Define CSS within the app for simplicity, or link to external file
    CSS = """
    Screen {
        layers: base overlay; /* Ensure overlay layers like dialogs work */
    }
    /* Styles for specific screen containers - Using explicit border properties */
    #setup-form, #dataset-container, #options-form, #summary-container, #transfer-container {
        padding: 1 2;
        /* Explicit border properties instead of shorthand */
        border-type: thick;
        border-color: $accent;
        margin: 1;
        overflow-y: auto;
        height: 100%;
    }
    /* Set titles separately */
    #setup-form { border-title: "Host & SSH"; }
    #dataset-container { border-title: "Dataset Selection"; }
    #options-form { border-title: "Options"; }
    #summary-container { border-title: "Summary & Confirmation"; }
    #transfer-container { border-title: "Transfer Progress"; }

    /* General Widget Styles */
    Label, .section-title {
        margin-top: 1;
        margin-bottom: 1;
        text-style: bold;
    }
    Button {
        margin-top: 1;
    }
    Input {
        width: 100%;
    }
    Checkbox {
        margin-top: 1;
        width: 100%;
    }
    #status-message, #final-status {
        margin-top: 1;
        text-align: center;
        height: auto;
        color: $text; /* Default color */
    }
    .status-hidden {
        display: none;
    }
    LoadingIndicator { /* General style for loading indicators */
        margin-top: 1;
        width: 100%;
        content-align: center middle; /* Corrected: Added vertical alignment */
    }
    /* Styles specific to DatasetScreen */
    #src-dataset-scroll, #dst-dataset-scroll {
        height: 10; /* Adjust height as needed */
        border: round $accent;
        margin-bottom: 1;
    }
    #dataset-container SelectionList { /* Target SelectionList within dataset screen */
         border: none;
    }
    /* Styles specific to SummaryScreen */
    #summary-table {
        margin-top: 1;
        height: auto; /* Adjust based on number of rows */
        border: round $accent;
    }
    #summary-buttons {
        align: center middle;
        margin-top: 1;
        width: 100%;
    }
    #summary-buttons Button {
        margin-left: 1;
        margin-right: 1;
    }
    /* Styles specific to TransferScreen */
    #transfer-container Log {
        /* Calculate height dynamically? For now, use fraction */
        height: 7fr; /* Give log most space */
        border: round $accent;
        margin-bottom: 1;
    }
    #transfer-container ProgressBar {
        height: 1fr; /* Progress bar height */
        margin-bottom: 1;
    }
     #transfer-container #final-status {
        height: 1fr; /* Final status height */
    }
    """
    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", priority=True),
    ]

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config # Store the global config
        self.initial_state = _load_interactive_state(STATE_FILE_PATH)
        self.final_job_config: Optional[Dict] = None
        # TODO: Add logic to handle command-line job selection vs interactive

    def on_mount(self) -> None:
        """Called when the app is first mounted."""
        # For now, always start interactive setup
        self.push_screen(HostSSHScreen(config=self.config, initial_state=self.initial_state))

    # --- Action Handlers ---
    def action_quit(self) -> None:
        """Quit the application."""
        # TODO: Add confirmation or cleanup if needed, e.g., stop workers
        self.exit(self.final_job_config) # Return the final config if available

    # --- Message Handlers ---
    def on_start_transfer(self, message: StartTransfer) -> None:
        """Handle the message to start the transfer."""
        logging.info(f"Received StartTransfer message for job: {message.job_config['_job_name']}")
        self.final_job_config = message.job_config # Store the config
        # Switch to the transfer screen
        # Clear previous setup screens first?
        while len(self.screen_stack) > 1: # Keep the base screen
             self.pop_screen()
        self.push_screen(TransferScreen(job_config=message.job_config))

# Note: The __main__ block for testing is removed as the app should be run via zfs_sync.py