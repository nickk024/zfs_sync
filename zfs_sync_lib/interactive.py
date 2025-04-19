import logging
import sys
from typing import Optional

# Rich for TUI elements
from rich.console import Console
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.panel import Panel
from rich.live import Live
from rich.table import Table

# Import necessary functions from other modules
from .utils import verify_ssh, execute_command
from .zfs import has_dataset # We might need more zfs functions

console = Console()

def get_datasets_interactive(host: str, ssh_user: str, config: dict) -> list[str]:
    """Fetches datasets from a host for interactive selection."""
    logging.debug(f"Fetching datasets from {host} for interactive list...")
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
        return []

def select_dataset_interactive(host: str, ssh_user: str, config: dict, prompt_title: str) -> Optional[str]:
    """Presents an interactive TUI list for dataset selection using Rich."""
    datasets = get_datasets_interactive(host, ssh_user, config)
    if not datasets:
        console.print(f"[bold red]Error:[/bold red] Could not retrieve datasets from {host}.")
        return None

    table = Table(title=prompt_title, show_header=False, show_lines=False, box=None)
    table.add_column("Dataset", style="cyan")

    current_selection = 0

    def generate_table() -> Table:
        table = Table(title=prompt_title, show_header=False, show_lines=False, box=None, expand=True)
        table.add_column("Dataset")
        for i, dataset in enumerate(datasets):
            if i == current_selection:
                table.add_row(f"> [bold cyan reverse] {dataset} [/bold cyan reverse]")
            else:
                table.add_row(f"  {dataset}")
        return table

    with Live(generate_table(), refresh_per_second=10, screen=True, transient=True) as live:
        while True:
            # Read a single character (requires platform-specific handling or a library)
            # Using rich's input handling via Prompt is easier than raw TTY manipulation
            # However, rich Prompt doesn't easily support arrow key navigation *within* a custom display.
            # Let's fall back to a simpler numbered list with rich Prompt for now.
            # TODO: Implement true arrow key navigation if essential (more complex)

            console.print(f"\n[bold blue]{prompt_title}[/bold blue]")
            for i, dataset in enumerate(datasets):
                console.print(f"  [cyan]{i+1}[/cyan]) {dataset}")
            console.print("  [bold]M[/bold]) Enter Manually")

            choice = Prompt.ask("Select dataset number or 'M'", default=str(current_selection + 1))

            if choice.lower() == 'm':
                manual_dataset = Prompt.ask("Enter dataset name")
                # Optional: Validate dataset exists?
                return manual_dataset.strip()
            elif choice.isdigit():
                index = int(choice) - 1
                if 0 <= index < len(datasets):
                    return datasets[index]
                else:
                    console.print("[prompt.invalid]Invalid selection.")
            else:
                 console.print("[prompt.invalid]Invalid input.")


def run_interactive_setup(config: dict, run_job_func):
    """Runs the interactive setup process and then executes the job."""
    console.print(Panel("[bold cyan]ZFS Sync - Interactive Setup[/bold cyan]", expand=False))

    # --- Gather Parameters ---
    try:
        # Use Rich prompts with defaults from config
        interactive_src_host = Prompt.ask("Enter Source Host", default=config.get('DEFAULT_SOURCE_HOST', 'local'))
        interactive_dst_host = Prompt.ask("Enter Destination Host", default=config.get('DEFAULT_DEST_HOST', ''))
        interactive_ssh_user = Prompt.ask("Enter SSH User", default=config.get('DEFAULT_SSH_USER', 'root'))

        if not interactive_dst_host:
            console.print("[bold red]Error:[/bold red] Destination Host is required.")
            sys.exit(1)

        # Verify SSH
        console.print("Verifying SSH connectivity...")
        if not verify_ssh(interactive_src_host, interactive_ssh_user, config): sys.exit(1)
        if not verify_ssh(interactive_dst_host, interactive_ssh_user, config): sys.exit(1)
        console.print("[green]SSH connectivity verified.[/green]")

        # Select Source Dataset
        interactive_src_dataset = select_dataset_interactive(
            interactive_src_host, interactive_ssh_user, config, "Select Source Dataset"
        )
        if not interactive_src_dataset: sys.exit(1)

        # Destination Dataset
        suggested_dst_dataset = config.get('DEFAULT_DEST_DATASET', interactive_src_dataset)
        interactive_dst_dataset = Prompt.ask("Enter Destination Dataset", default=suggested_dst_dataset)
        if not interactive_dst_dataset:
             console.print("[bold red]Error:[/bold red] Destination Dataset is required.")
             sys.exit(1)

        # Other Options
        interactive_recursive = Confirm.ask("Recursive transfer?", default=config.get('DEFAULT_RECURSIVE', True))
        interactive_compression = Confirm.ask("Use compression?", default=config.get('DEFAULT_USE_COMPRESSION', True))
        interactive_resume = Confirm.ask("Enable resume support?", default=config.get('DEFAULT_RESUME_SUPPORT', True))
        interactive_direct_remote = Confirm.ask("Direct remote-to-remote transfer?", default=config.get('DEFAULT_DIRECT_REMOTE_TRANSFER', False))

        interactive_snapshot_prefix = Prompt.ask("Enter Snapshot Prefix", default=config.get('DEFAULT_SNAPSHOT_PREFIX', 'backup'))
        interactive_max_snapshots = IntPrompt.ask("Enter Max Snapshots to keep", default=config.get('DEFAULT_MAX_SNAPSHOTS', 5))

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
            'sync_snapshot': f"{interactive_snapshot_prefix}-sync"
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
        console.print(summary_table)

        # --- Confirm and Run ---
        if Confirm.ask("Proceed with this configuration?", default=True):
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