#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the virtual environment directory
VENV_DIR="venv"

# Check if the virtual environment directory exists
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in '$VENV_DIR'..."
  python3 -m venv "$VENV_DIR"
  echo "Virtual environment created."
else
  echo "Virtual environment '$VENV_DIR' already exists."
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Install/update dependencies
echo "Installing/updating dependencies from requirements.txt..."
pip install -r requirements.txt

# Run the application
echo "Starting ZFS Sync Tool..."
python -m zfs_sync_tool.core.app

# Deactivate virtual environment upon exit (optional, script termination handles this)
# echo "Deactivating virtual environment..."
# deactivate

echo "ZFS Sync Tool finished."
exit 0