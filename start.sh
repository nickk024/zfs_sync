#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the virtual environment directory name
VENV_DIR=".venv"
PYTHON_CMD="python3" # Use python3

# Check if Git repository exists, initialize if not
if [ ! -d ".git" ]; then
  echo "Initializing Git repository..."
  git init
else
  echo "Git repository already initialized."
fi

# Check if the virtual environment directory exists
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in $VENV_DIR..."
  $PYTHON_CMD -m venv $VENV_DIR
  echo "Virtual environment created."
else
  echo "Virtual environment '$VENV_DIR' already exists."
fi

# Activate the virtual environment
# Note: Activation changes the current shell's state,
# subsequent commands in the script run within the venv.
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Install/update dependencies
echo "Installing/updating dependencies from requirements.txt..."
pip install -r requirements.txt

# Ensure sanoid/syncoid scripts are executable (if they exist)
if [ -f "libs/sanoid/sanoid" ]; then
    chmod +x libs/sanoid/sanoid
fi
if [ -f "libs/sanoid/syncoid" ]; then
    chmod +x libs/sanoid/syncoid
fi


# Run the main Python script, passing all script arguments to it
echo "Running zfs_sync.py..."
$PYTHON_CMD zfs_sync.py "$@"

# Deactivation is usually handled when the script or shell exits
# echo "Deactivating virtual environment..."
# deactivate # Optional: uncomment if explicit deactivation is needed within the script's context

echo "Script finished."