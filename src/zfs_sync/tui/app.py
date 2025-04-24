"""
Main TUI application for ZFS Sync Tool
"""

import logging
from textual.app import App
from textual.widgets import Header, Footer
from textual.binding import Binding

from zfs_sync.tui.screens.main_screen import MainScreen

logger = logging.getLogger('zfs_sync.tui')

class ZFSSyncApp(App):
    """Main TUI application for ZFS Sync Tool."""
    
    TITLE = "ZFS Sync Tool"
    SUB_TITLE = "Synchronize ZFS datasets between servers using snapshots"
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("d", "toggle_dark", "Toggle Dark Mode"),
        Binding("h", "show_help", "Help"),
    ]
    
    def __init__(self, *args, initial_job=None, **kwargs):
        super().__init__(*args, **kwargs)
        logger.debug("Initializing ZFS Sync App")
        self.initial_job = initial_job
    
    async def on_mount(self) -> None:
        """Called when the app is mounted."""
        logger.debug("App mounted")
        
        # Load the main screen with the initial job if specified
        if self.initial_job:
            logger.debug(f"Loading main screen with initial job: {self.initial_job}")
            await self.push_screen(MainScreen(initial_job=self.initial_job))
        else:
            await self.push_screen(MainScreen())
    
    async def action_toggle_dark(self) -> None:
        """Toggle dark mode."""
        self.dark = not self.dark
    
    async def action_show_help(self) -> None:
        """Show help screen."""
        # TODO: Implement help screen
        self.notify("Help screen not yet implemented")