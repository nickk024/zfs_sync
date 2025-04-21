import logging
from typing import Optional, Dict, Any
from textual.message import Message

class StartTransfer(Message):
    """Message to signal starting the transfer process."""
    def __init__(self, job_config: Dict[str, Any]):
        self.job_config = job_config
        super().__init__()

class LogMessage(Message):
    """Message to log text to the TransferScreen."""
    def __init__(self, text: str, level: int = logging.INFO):
        self.text = text
        self.level = level
        super().__init__()

class ProgressUpdate(Message):
    """Message to update the progress bar."""
    def __init__(self, completed: int, total: Optional[int], action: Optional[str] = None):
        self.completed = completed
        self.total = total
        self.action = action
        super().__init__()

class TransferFinished(Message):
    """Message indicating the transfer worker has finished."""
    def __init__(self, success: bool):
        self.success = success
        super().__init__()

# Add other TUI-specific messages here if needed in the future