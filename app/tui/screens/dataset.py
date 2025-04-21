import logging
from typing import Dict, Any, Optional, List, Tuple, Callable

from textual.app import ComposeResult
from textual.containers import Container, VerticalScroll
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Button, Input, Label, LoadingIndicator, SelectionList
from textual.reactive import reactive
from textual.binding import Binding
from textual.widgets.selection_list import Selection

# Import necessary functions/constants/screens
from ...zfs import get_datasets
from .options import OptionsScreen # Import next screen

# Define constants locally for now, or move to a shared constants file later
MANUAL_ENTRY_ID = "__manual__"

class DatasetScreen(Screen):
    """Screen for selecting source and destination datasets."""
    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=True),
        Binding("ctrl+q", "app.quit", "Quit", show=False),
    ]

    # Reactive state for datasets
    src_datasets = reactive[List[Tuple[str, str]]]([])
    dst_datasets = reactive[List[Tuple[str, str]]]([])
    loading_src = reactive(False)
    loading_dst = reactive(False)
    selected_src_dataset = reactive[Optional[str]](None)
    selected_dst_dataset = reactive[Optional[str]](None)

    def __init__(self, config: Dict[str, Any], job_config_so_far: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.job_config_so_far = job_config_so_far
        self.current_selection_list_id = "#src-dataset-list" # Track which list is active

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="dataset-container"):
            yield Static("Select Source Dataset:", classes="section-title")
            yield LoadingIndicator(id="src-loading", classes="status-hidden")
            with VerticalScroll(id="src-dataset-scroll"):
                 yield SelectionList[str](id="src-dataset-list") # Selection value is str
            yield Static("Select Destination Dataset:", classes="section-title")
            yield LoadingIndicator(id="dst-loading", classes="status-hidden")
            with VerticalScroll(id="dst-dataset-scroll"):
                 yield SelectionList[str](id="dst-dataset-list") # Selection value is str
            yield Static() # Spacer
            yield Input(placeholder="Or enter destination dataset manually", id="dst-manual-input", classes="status-hidden")
            yield Static(id="status-message", classes="status-hidden")
            yield Button("Continue", variant="primary", id="button-continue", disabled=True)
        yield Footer()

    def on_mount(self) -> None:
        """Set title and start loading datasets when the screen is mounted."""
        try:
            container = self.query_one("#dataset-container", Container)
            container.border_title = "Dataset Selection"
        except Exception as e:
            logging.error(f"Error setting border title for #dataset-container: {e}") # Add logging
        self.fetch_src_datasets()
        # We fetch dst datasets after src is selected or if src==dst

    # --- Data Fetching ---
    def fetch_src_datasets(self) -> None:
        """Fetch source datasets in a worker."""
        self.loading_src = True
        self.query_one("#src-loading", LoadingIndicator).remove_class("status-hidden")
        self.query_one("#src-dataset-list", SelectionList).disabled = True
        self.app.run_worker(
            self.load_datasets_worker(
                self.job_config_so_far['source_host'],
                self.job_config_so_far['ssh_user'],
                self.update_src_list # Callback
            ),
            exclusive=True,
            group="dataset_loading"
        )

    def fetch_dst_datasets(self) -> None:
        """Fetch destination datasets in a worker."""
        # Only fetch if dest host is different from source
        if self.job_config_so_far['source_host'] == self.job_config_so_far['dest_host']:
            self.dst_datasets = self.src_datasets # Use the same list
            self.update_dst_list(self.dst_datasets) # Update UI directly
            return

        self.loading_dst = True
        self.query_one("#dst-loading", LoadingIndicator).remove_class("status-hidden")
        self.query_one("#dst-dataset-list", SelectionList).disabled = True
        self.app.run_worker(
            self.load_datasets_worker(
                self.job_config_so_far['dest_host'],
                self.job_config_so_far['ssh_user'],
                self.update_dst_list # Callback
            ),
            exclusive=True,
            group="dataset_loading"
        )

    async def load_datasets_worker(self, host: str, user: str, callback: Callable[[List[Tuple[str, str]], Optional[str]], None]):
        """Worker to load datasets and call the UI update callback."""
        datasets_tuples: List[Tuple[str, str]] = []
        error_msg = None
        try:
            datasets = await self.app.run_sync_in_worker_thread(
                get_datasets, host, user, self.config
            )
            datasets_tuples = [(ds, ds) for ds in datasets] # Format for SelectionList (value, label)
            datasets_tuples.append((MANUAL_ENTRY_ID, "(Manual Entry)")) # Add manual option
        except Exception as e:
            error_msg = f"Failed to load datasets from {host}: {e}"
            logging.exception(f"Dataset loading error from {host}")

        self.app.call_from_thread(callback, datasets_tuples, error_msg)

    # --- UI Update Callbacks ---
    def update_src_list(self, datasets: List[Tuple[str, str]], error: Optional[str] = None):
        """Update the source dataset SelectionList."""
        self.loading_src = False
        self.query_one("#src-loading", LoadingIndicator).add_class("status-hidden")
        src_list = self.query_one("#src-dataset-list", SelectionList)
        src_list.clear_options()
        if error:
            self.query_one("#status-message").update(f"[red]Error: {error}[/]")
            self.query_one("#status-message").remove_class("status-hidden")
            # Maybe allow retry? For now, disable list.
        else:
            src_list.add_options([(label, value) for value, label in datasets]) # Note: Textual uses (label, value)
            src_list.disabled = False
            self.src_datasets = datasets # Store the raw data if needed
            self.query_one("#status-message").add_class("status-hidden") # Clear previous errors

    def update_dst_list(self, datasets: List[Tuple[str, str]], error: Optional[str] = None):
        """Update the destination dataset SelectionList."""
        self.loading_dst = False
        self.query_one("#dst-loading", LoadingIndicator).add_class("status-hidden")
        dst_list = self.query_one("#dst-dataset-list", SelectionList)
        dst_list.clear_options()
        if error:
            # Log error but don't necessarily block manual entry
            self.query_one("#status-message").update(f"[yellow]Warning: {error}. Manual entry still possible.[/]")
            self.query_one("#status-message").remove_class("status-hidden")
            # Enable manual input if list fails
            self.query_one("#dst-manual-input").remove_class("status-hidden")
            dst_list.disabled = True # Disable list if loading failed
        else:
            dst_list.add_options([(label, value) for value, label in datasets])
            dst_list.disabled = False
            self.dst_datasets = datasets
            # Hide manual input if list loaded successfully initially
            self.query_one("#dst-manual-input").add_class("status-hidden")
            self.query_one("#status-message").add_class("status-hidden") # Clear previous errors
        self.check_continue_button_state() # Check if continue can be enabled

    # --- Event Handlers ---
    def on_selection_list_selection_changed(self, event: SelectionList.SelectedChanged) -> None: # Corrected event name
        """Handle changes in dataset selections."""
        list_id = event.selection_list.id
        selected_values = event.selection_list.selected # This is a list of values

        if not selected_values: # Nothing selected / deselected
             if list_id == "src-dataset-list":
                 self.selected_src_dataset = None
             elif list_id == "dst-dataset-list":
                 self.selected_dst_dataset = None
                 self.query_one("#dst-manual-input").add_class("status-hidden") # Hide manual if list item deselected
             self.check_continue_button_state()
             return

        selected_value = selected_values[0] # Single selection mode

        if list_id == "src-dataset-list":
            if selected_value == MANUAL_ENTRY_ID:
                 # TODO: Handle manual source entry? Less common, maybe add later.
                 self.query_one("#status-message").update("[yellow]Manual source entry not fully implemented yet.[/]")
                 self.query_one("#status-message").remove_class("status-hidden")
                 self.selected_src_dataset = None # Reset selection
                 event.selection_list.deselect(MANUAL_ENTRY_ID) # Deselect manual entry visually
            else:
                self.selected_src_dataset = selected_value
                # If a source is selected, trigger loading destination datasets
                self.fetch_dst_datasets()

        elif list_id == "dst-dataset-list":
            dst_manual_input = self.query_one("#dst-manual-input", Input)
            if selected_value == MANUAL_ENTRY_ID:
                self.selected_dst_dataset = None # Clear selection if manual is chosen
                dst_manual_input.remove_class("status-hidden")
                dst_manual_input.focus()
            else:
                self.selected_dst_dataset = selected_value
                dst_manual_input.add_class("status-hidden") # Hide manual input
                dst_manual_input.value = "" # Clear manual input

        self.check_continue_button_state()

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle changes in the manual destination input."""
        if event.input.id == "dst-manual-input":
            self.check_continue_button_state()

    def check_continue_button_state(self) -> None:
        """Enable continue button only if both source and dest are selected/entered."""
        dst_manual_value = self.query_one("#dst-manual-input", Input).value.strip()
        # Enable if src is selected AND (dst is selected OR manual input has value)
        can_continue = bool(self.selected_src_dataset and (self.selected_dst_dataset or dst_manual_value))
        self.query_one("#button-continue", Button).disabled = not can_continue

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle continue button press."""
        if event.button.id == "button-continue":
            dst_dataset = self.selected_dst_dataset or self.query_one("#dst-manual-input", Input).value.strip()
            if not dst_dataset: # Should be caught by button state, but double check
                self.query_one("#status-message").update("[red]Destination dataset cannot be empty.[/]")
                self.query_one("#status-message").remove_class("status-hidden")
                return

            # Update job config
            self.job_config_so_far['source_dataset'] = self.selected_src_dataset
            self.job_config_so_far['dest_dataset'] = dst_dataset

            # Proceed to the next screen
            self.app.push_screen(OptionsScreen(config=self.config, job_config_so_far=self.job_config_so_far))