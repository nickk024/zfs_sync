import logging
import sys
from typing import Optional, List, Tuple

# Rich for TUI elements (still used for other prompts and output)
from rich.console import Console
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.panel import Panel
from rich.table import Table

# Prompt Toolkit for interactive list selection
from prompt_toolkit.shortcuts import radiolist_dialog, input_dialog, message_dialog
from prompt_toolkit.styles import Style

# Import necessary functions from other modules
from .utils import verify_ssh, execute_command
# from .zfs import has_dataset # Not currently used here

console = Console()

# Define a simple style for prompt_toolkit dialogs
dialog_style = Style.from_dict({
    'dialog':             'bg:#444444 #ffffff',
    'dialog frame.label': 'bg:#ffffff #000000',
    'dialog.body':        'bg:#888888 #000000',
    'button':             'bg:#000000 #ffffff',
})

def get_datasets_interactive(host: str, ssh_user: str, config: dict) -> List[str]:
    """Fetches datasets from a host for interactive selection."""
    logging.debug(f"Fetching datasets from {host} for interactive list...")
    console.print(f"Fetching datasets from [cyan]{host}[/]...")
    try:
        # Get datasets, limit depth for clarity? Maybe -d 1?
        cmd_result = execute_command(
            ['zfs', 'list', '-o', 'name', '-H', '-t', 'filesystem,volume'], # List filesystems and volumes
            host=host, ssh_user=ssh_user, config=config, check=True, capture_output=True
        )
        datasets = cmd_result.stdout.strip().splitlines()
        # Optional: Filter out parent datasets if children exist? Might be complex.
        # For now, return all found.
        logging.debug(f"Found {len(datasets)} datasets on {host}.")
        return datasets
    except Exception as e:
        logging.error(f"Failed to list datasets on {host}: {e}")
        console.print(f"[bold red]Error:[/bold red] Failed to list datasets on {host}: {e}")
        return []

def select_dataset_interactive(host: str, ssh_user: str, config: dict, prompt_title: str) -> Optional[str]:
    """Presents an interactive TUI list for dataset selection using prompt_toolkit."""
    datasets = get_datasets_interactive(host, ssh_user, config)
    if not datasets:
        message_dialog(
            title="Error",
            text=f"Could not retrieve datasets from {host}.",
            style=dialog_style
        ).run()
        return None

    # Prepare choices for radiolist_dialog: (value, label)
    choices: List[Tuple[str, str]] = [(ds, ds) for ds in datasets]
    manual_entry_value = "__manual__"
    choices.append((manual_entry_value, "(Manual Entry)"))

    selected_value = radiolist_dialog(
        title=prompt_title,
        text="Select a dataset using arrow keys and Enter:",
        values=choices,
        style=dialog_style
    ).run()

    if selected_value is None:
        # User pressed Cancel/Escape
        return None
    elif selected_value == manual_entry_value:
        # User selected Manual Entry
        manual_dataset = input_dialog(
            title="Manual Dataset Entry",
            text=f"Enter the full dataset name on {host}:",
            style=dialog_style
        ).run()
        # Return the entered value (or None if cancelled)
        return manual_dataset.strip() if manual_dataset else None
    else:
        # User selected a dataset from the list
        return selected_value


def run_interactive_setup(config: dict, run_job_func):
    """Runs the interactive setup process and then executes the job."""
    console.print(Panel("[bold cyan]ZFS Sync - Interactive Setup[/bold cyan]", expand=False))

    # --- Gather Parameters ---
    try:
        # --- Host and SSH Configuration ---
        console.print(Panel("[bold]Host & SSH Configuration[/bold]", expand=False, border_style="blue"))
        interactive_src_host = Prompt.ask("Enter Source Host", default=config.get('DEFAULT_SOURCE_HOST', 'local'))
        interactive_dst_host = Prompt.ask("Enter Destination Host", default=config.get('DEFAULT_DEST_HOST', ''))
        interactive_ssh_user = Prompt.ask("Enter SSH User", default=config.get('DEFAULT_SSH_USER', 'root'))

        if not interactive_dst_host:
            console.print("[bold red]Error:[/bold red] Destination Host is required.")
            sys.exit(1)

        # Verify SSH
        console.print(f"\nVerifying SSH connectivity to [cyan]{interactive_src_host}[/] and [cyan]{interactive_dst_host}[/]...")
        if not verify_ssh(interactive_src_host, interactive_ssh_user, config): sys.exit(1)
        if interactive_src_host != interactive_dst_host and not verify_ssh(interactive_dst_host, interactive_ssh_user, config): sys.exit(1)
        console.print("[green]SSH connectivity verified.[/green]\n")

        # --- Dataset Selection ---
        console.print(Panel("[bold]Dataset Selection[/bold]", expand=False, border_style="blue"))
        # Select Source Dataset using prompt_toolkit
        interactive_src_dataset = select_dataset_interactive(
            interactive_src_host, interactive_ssh_user, config, "Select Source Dataset"
        )
        if not interactive_src_dataset:
            console.print("[yellow]Source dataset selection cancelled.[/yellow]")
            sys.exit(1)
        console.print(f"Selected source dataset: [cyan]{interactive_src_dataset}[/]")

        # Select Destination Dataset using prompt_toolkit (or suggest based on source)
        suggested_dst_dataset = config.get('DEFAULT_DEST_DATASET', interactive_src_dataset)
        console.print(f"\nDestination dataset on [cyan]{interactive_dst_host}[/]:")
        use_suggestion = Confirm.ask(f"Use suggested destination '[cyan]{suggested_dst_dataset}[/]'?", default=True)

        if use_suggestion:
            interactive_dst_dataset = suggested_dst_dataset
        else:
            interactive_dst_dataset = select_dataset_interactive(
                interactive_dst_host, interactive_ssh_user, config, "Select Destination Dataset"
            )
            if not interactive_dst_dataset:
                console.print("[yellow]Destination dataset selection cancelled.[/yellow]")
                sys.exit(1)
        console.print(f"Selected destination dataset: [cyan]{interactive_dst_dataset}[/]\n")

        # --- Transfer Options ---
        console.print(Panel("[bold]Transfer Options[/bold]", expand=False, border_style="blue"))
        interactive_recursive = Confirm.ask("Recursive transfer?", default=config.get('DEFAULT_RECURSIVE', True))
        interactive_compression = Confirm.ask("Use compression during transfer?", default=config.get('DEFAULT_USE_COMPRESSION', True))
        interactive_resume = Confirm.ask("Enable resume support for initial transfer?", default=config.get('DEFAULT_RESUME_SUPPORT', True))
        # interactive_direct_remote = Confirm.ask("Direct remote-to-remote transfer (if applicable)?", default=config.get('DEFAULT_DIRECT_REMOTE_TRANSFER', False)) # Removed R2R
        interactive_direct_remote = False # Hardcode to False as R2R is removed
        console.print("") # Spacer

        # --- Snapshot Options ---
        console.print(Panel("[bold]Snapshot Options[/bold]", expand=False, border_style="blue"))
        interactive_snapshot_prefix = Prompt.ask("Enter Snapshot Prefix", default=config.get('DEFAULT_SNAPSHOT_PREFIX', 'zfs-sync'))
        interactive_max_snapshots = IntPrompt.ask("Enter Max Snapshots to keep on destination", default=config.get('DEFAULT_MAX_SNAPSHOTS', 5))
        console.print("") # Spacer

        # --- Execution Options ---
        console.print(Panel("[bold]Execution Options[/bold]", expand=False, border_style="blue"))
        interactive_dry_run = Confirm.ask("Perform a dry run (show commands without executing)?", default=False)
        console.print("") # Spacer

        # --- Create Job Config Dictionary ---
        interactive_job_config = {
            '_job_name': 'interactive', # Special name for interactive job
            'source_host': interactive_src_host,
            'source_dataset': interactive_src_dataset,
            'dest_host': interactive_dst_host,
            'dest_dataset': interactive_dst_dataset,
            'ssh_user': interactive_ssh_user,
            'snapshot_prefix': interactive_snapshot_prefix,
            'max_snapshots': interactive_max_snapshots,
            'recursive': interactive_recursive,
            'use_compression': interactive_compression,
            'resume_support': interactive_resume,
            'direct_remote_transfer': interactive_direct_remote,
            'sync_snapshot': f"{interactive_snapshot_prefix}-sync", # Intermediate snapshot name
            'dry_run': interactive_dry_run
        }

        # --- Display Summary ---
        summary_table = Table(title="Configuration Summary", box=None, show_header=False)
        summary_table.add_column("Parameter", style="dim")
        summary_table.add_column("Value")
        summary_table.add_row("Source", f"{interactive_ssh_user}@{interactive_src_host}:{interactive_src_dataset}")
        summary_table.add_row("Destination", f"{interactive_ssh_user}@{interactive_dst_host}:{interactive_dst_dataset}")
        summary_table.add_row("Recursive", str(interactive_recursive))
        summary_table.add_row("Compression", str(interactive_compression))
        summary_table.add_row("Resume Support", str(interactive_resume))
        summary_table.add_row("Direct Remote", str(interactive_direct_remote))
        summary_table.add_row("Snapshot Prefix", interactive_snapshot_prefix)
        summary_table.add_row("Max Snapshots", str(interactive_max_snapshots))
        summary_table.add_row("Dry Run", "[yellow]Yes[/yellow]" if interactive_dry_run else "No")
        console.print(summary_table)

        # --- Confirm and Run ---
        if Confirm.ask("Proceed with this configuration?", default=True):
            if interactive_dry_run:
                console.print("[bold yellow]Starting dry run...[/bold yellow]")
            else:
                console.print("[bold green]Starting synchronization...[/bold green]")
            # Call the run_job function passed from the main script
            success = run_job_func(interactive_job_config, config)
            sys.exit(0 if success else 1)
        else:
            console.print("[yellow]Aborted by user.[/yellow]")
            sys.exit(1)

    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Interactive setup aborted.[/yellow]")
        sys.exit(1)
    except Exception as e:
        logging.exception("Error during interactive setup.") # Log traceback
        console.print(f"[bold red]An error occurred during setup: {e}[/bold red]")
        sys.exit(1)