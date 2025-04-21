import logging
import sys
from zfs_sync_tool.config import manager as config_manager
from zfs_sync_tool.zfs import interface as zfs_interface
from zfs_sync_tool.sanoid import interface as sanoid_interface
# Import other components like ssh, scheduler, notifications, tui as needed

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
        self._configure_logging()
        logger.info("ZFS Sync Tool application initialized.")
        logger.debug(f"Loaded configuration: {self.config}")

        # Initialize interfaces (can be expanded later)
        self.zfs = zfs_interface
        self.sanoid = sanoid_interface
        # self.ssh = ...
        # self.scheduler = ...
        # self.notifier = ...
        # self.tui = ...

    def _configure_logging(self):
        """Configures logging based on the loaded configuration."""
        log_level_str = self.config.get("general", {}).get("log_level", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        log_file = self.config.get("general", {}).get("log_file")

        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        # Clear existing handlers (optional, prevents duplicate logs if run multiple times)
        # for handler in root_logger.handlers[:]:
        #     root_logger.removeHandler(handler)

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # File Handler (if specified)
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                logger.info(f"Logging to file: {log_file}")
            except IOError as e:
                logger.error(f"Failed to configure file logging to {log_file}: {e}")
        else:
            logger.info("File logging is disabled.")


    def run(self):
        """Starts the main application execution (e.g., launches TUI or runs tasks)."""
        logger.info("Starting ZFS Sync Tool application run...")

        # --- Placeholder for main logic ---
        # This is where you would typically:
        # 1. Initialize the TUI
        # 2. Start the scheduler
        # 3. Perform initial checks (ZFS/Sanoid availability, config validation)
        # 4. Enter the main application loop (e.g., tui.run())

        print("ZFS Sync Tool Application - Core Logic Placeholder")
        print("Listing ZFS filesystems as an example:")
        try:
            filesystems = self.zfs.list_datasets("filesystem")
            if filesystems:
                for fs in filesystems:
                    print(f"- {fs}")
            else:
                print("No filesystems found or ZFS not available.")
        except Exception as e:
            logger.exception("Error listing filesystems during initial run.")
            print(f"Error listing filesystems: {e}")

        logger.info("ZFS Sync Tool application run finished (placeholder).")


def main(config_path: str = None):
    """Main entry point function."""
    try:
        app = ZfsSyncApp(config_path)
        app.run()
    except Exception as e:
        logging.exception("An unhandled exception occurred during application execution.")
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    # Example: Run the application directly
    # You could add command-line argument parsing here (e.g., using argparse)
    # to specify a config file path: main(args.config)
    main()