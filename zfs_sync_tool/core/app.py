import logging
import sys
from zfs_sync_tool.config import manager as config_manager
from zfs_sync_tool.zfs import interface as zfs_interface
from zfs_sync_tool.sanoid import interface as sanoid_interface
# Import TUI App
from zfs_sync_tool.tui.app import ZfsSyncTuiApp
# Import other components like ssh, scheduler, notifications as needed

logger = logging.getLogger(__name__)

class ZfsSyncApp:
    """Main application class."""

    def __init__(self, config_path: str = None):
        """
        Initializes the application.

        Args:
            config_path: Optional path to a specific configuration file.
        """
        self.config = config_manager.load_config(config_path)
        # Configure logging *before* doing anything else that might log
        self._configure_logging()
        logger.info("ZFS Sync Tool application initialized.")
        logger.debug(f"Loaded configuration: {self.config}")

        # Initialize interfaces (can be expanded later)
        self.zfs = zfs_interface
        self.sanoid = sanoid_interface
        # self.ssh = ...
        # self.scheduler = ...
        # self.notifier = ...
        # self.tui = None # TUI instance created in run()

    def _configure_logging(self):
        """Configures logging based on the loaded configuration."""
        log_level_str = self.config.get("general", {}).get("log_level", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        log_file = self.config.get("general", {}).get("log_file")

        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)

        # Configure root logger
        # Get logger instance for the entire application module space if desired
        # Or configure the root logger directly
        app_logger = logging.getLogger('zfs_sync_tool') # Get logger for our package
        app_logger.setLevel(log_level)

        # Prevent duplicate handlers if this is called multiple times or by libraries
        if not app_logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            app_logger.addHandler(console_handler)

            # File Handler (if specified)
            if log_file:
                try:
                    # Use RotatingFileHandler for production?
                    file_handler = logging.FileHandler(log_file, encoding='utf-8')
                    file_handler.setFormatter(formatter)
                    app_logger.addHandler(file_handler)
                    logger.info(f"Logging to file: {log_file}")
                except IOError as e:
                    logger.error(f"Failed to configure file logging to {log_file}: {e}", exc_info=True)
            else:
                logger.info("File logging is disabled.")
        # Ensure root logger level is also set if libraries use it directly
        logging.getLogger().setLevel(log_level)


    def run(self):
        """Starts the main application execution by launching the TUI."""
        logger.info("Starting ZFS Sync Tool TUI...")

        # --- Initialize and run the TUI ---
        # Pass the instance of this core app to the TUI
        tui_app = ZfsSyncTuiApp(core_app=self)
        tui_app.run() # This blocks until the TUI exits

        # TUI has exited
        logger.info("ZFS Sync Tool TUI finished.")


def main(config_path: str = None):
    """Main entry point function."""
    # Basic logging setup until config is loaded
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app = None # Ensure app is defined in case of init error
    try:
        app = ZfsSyncApp(config_path)
        app.run()
        sys.exit(0) # Explicitly exit with success code
    except Exception as e:
        # Use the logger if the app initialized enough to configure it
        if app and hasattr(app, 'logger'):
             app.logger.exception("An unhandled exception occurred during application execution.")
        else:
             logging.exception("An unhandled exception occurred during application execution.")
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    # Example: Run the application directly
    # You could add command-line argument parsing here (e.g., using argparse)
    # to specify a config file path: main(args.config)
    main()