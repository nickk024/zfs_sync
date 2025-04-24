#!/bin/bash

# ZFS Sync Tool Start Script
# This script handles both setup and running of the ZFS Sync Tool.

set -e

# Function to display help
show_help() {
    echo "ZFS Sync Tool Start Script"
    echo "Usage: ./start.sh [options]"
    echo ""
    echo "Options:"
    echo "  --setup              Set up the ZFS Sync Tool (install dependencies, create virtual environment)"
    echo "  --create-job <name>  Create a new sync job with the specified name"
    echo "  --edit-job <name>    Edit an existing sync job"
    echo "  --run-job <name>     Run a specific sync job immediately"
    echo "  --list-jobs          List all available sync jobs"
    echo "  --list-schedules     List all scheduled jobs (cron jobs)"
    echo "  --help               Display this help message"
    echo ""
    echo "If no options are provided, the script will start the TUI interface."
}

# Function to set up the ZFS Sync Tool
setup() {
    echo "Setting up ZFS Sync Tool..."

    # Check if Python 3 is installed
    if ! command -v python3 &> /dev/null; then
        echo "Python 3 is required but not installed. Please install Python 3 and try again."
        exit 1
    fi

    # Check if pip is installed
    if ! command -v pip3 &> /dev/null; then
        echo "pip3 is required but not installed. Please install pip3 and try again."
        exit 1
    fi

    # Check if ZFS is installed
    if ! command -v zfs &> /dev/null; then
        echo "WARNING: ZFS is not installed. Some functionality may not work."
    fi

    # Check if virtualenv is installed
    if ! command -v virtualenv &> /dev/null; then
        echo "virtualenv is not installed. Installing..."
        pip3 install virtualenv
    fi

    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m virtualenv venv
    fi

    # Activate virtual environment
    echo "Activating virtual environment..."
    source venv/bin/activate

    # Install dependencies
    echo "Installing dependencies..."
    pip install -r requirements.txt || pip install textual paramiko

    # Initialize git repository if it doesn't exist
    if [ ! -d ".git" ]; then
        echo "Initializing git repository..."
        git init
        git add .
        git commit -m "Initial commit"
    fi

    # Create config directory
    echo "Creating config directory..."
    SANOID_CONFIG_DIR=~/.zfs_sync
    mkdir -p $SANOID_CONFIG_DIR

    # Create default sanoid configuration if it doesn't exist
    if [ ! -f "$SANOID_CONFIG_DIR/sanoid.conf" ]; then
        echo "Creating default sanoid configuration..."
        cat > $SANOID_CONFIG_DIR/sanoid.conf << EOF
# Sanoid configuration for ZFS Sync Tool

[tank/media]
    use_template = production

#############################
# templates below this line #
#############################

[template_production]
    frequently = 0
    hourly = 36
    daily = 30
    monthly = 3
    yearly = 0
    autosnap = yes
    autoprune = yes
EOF
        
        echo "Default sanoid configuration created at $SANOID_CONFIG_DIR/sanoid.conf"
        echo "Please edit this file to match your ZFS dataset configuration."
    fi

    # Create default sync configuration to ensure first sync is full and subsequent are incremental
    if [ ! -f "$SANOID_CONFIG_DIR/config.json" ]; then
        echo "Creating default sync configuration..."
        cat > $SANOID_CONFIG_DIR/config.json << EOF
{
    "version": 1,
    "default_source_dataset": "tank/media",
    "default_destination_server": "localhost",
    "default_destination_dataset": "backup/media",
    "sync_options": {
        "recursive": true,
        "compress": "lz4",
        "create-bookmark": true,
        "preserve-properties": true,
        "first_sync_full": true,
        "subsequent_sync_incremental": true
    },
    "sanoid": {
        "enabled": true,
        "config_path": "$SANOID_CONFIG_DIR/sanoid.conf"
    },
    "saved_configurations": [],
    "jobs": {},
    "scheduled_jobs": []
}
EOF
        
        echo "Default sync configuration created at $SANOID_CONFIG_DIR/config.json"
        echo "This configuration ensures the first sync is full and subsequent syncs are incremental."
    fi

    echo "Setup complete!"
}

# Function to create a cron job for a specific job
schedule_job() {
    local job_name=$1
    local schedule=$2
    local current_dir=$(pwd)
    
    # Add cron job
    (crontab -l 2>/dev/null; echo "$schedule cd $current_dir && ./start.sh --run-job $job_name") | crontab -
    
    # Update the config to track scheduled jobs
    local config_file=~/.zfs_sync/config.json
    if [ -f "$config_file" ]; then
        # Use Python to update the config file
        python3 -c "
import json
with open('$config_file', 'r') as f:
    config = json.load(f)
if 'scheduled_jobs' not in config:
    config['scheduled_jobs'] = []
# Add the job to scheduled_jobs if not already there
job_entry = {'name': '$job_name', 'schedule': '$schedule', 'command': 'cd $current_dir && ./start.sh --run-job $job_name'}
if job_entry not in config['scheduled_jobs']:
    config['scheduled_jobs'].append(job_entry)
with open('$config_file', 'w') as f:
    json.dump(config, f, indent=4)
"
    fi
    
    echo "Job '$job_name' scheduled with cron schedule: $schedule"
    echo "You can check your cron jobs with: crontab -l"
    echo "You can also view scheduled jobs in the TUI or with: ./start.sh --list-schedules"
}

# Function to list scheduled jobs
list_schedules() {
    echo "Scheduled jobs (from crontab):"
    crontab -l 2>/dev/null | grep -F "start.sh --run-job" || echo "No scheduled jobs found in crontab."
    
    echo
    echo "Scheduled jobs (from config):"
    local config_file=~/.zfs_sync/config.json
    if [ -f "$config_file" ]; then
        # Use Python to read and display scheduled jobs from config
        python3 -c "
import json
try:
    with open('$config_file', 'r') as f:
        config = json.load(f)
    if 'scheduled_jobs' in config and config['scheduled_jobs']:
        for i, job in enumerate(config['scheduled_jobs'], 1):
            print(f\"{i}. {job['name']} - Schedule: {job['schedule']}\")
    else:
        print('No scheduled jobs found in config.')
except Exception as e:
    print(f'Error reading config: {e}')
"
    else
        echo "Config file not found."
    fi
}

# Function to run the application
run_app() {
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        echo "Virtual environment not found. Running setup first..."
        setup
    fi

    # Activate virtual environment
    source venv/bin/activate

    # Run ZFS Sync Tool
    python -m src.zfs_sync.main "$@"

    # Deactivate virtual environment
    deactivate
}

# Parse command line arguments
if [ $# -eq 0 ]; then
    # No arguments, check if setup has been run
    if [ ! -d "venv" ]; then
        echo "First-time setup detected. Running setup..."
        setup
    fi
    
    # Run the application
    run_app
    exit 0
fi

case "$1" in
    --setup)
        setup
        ;;
    --help)
        show_help
        ;;
    --create-job)
        if [ -z "$2" ]; then
            echo "ERROR: Job name is required"
            show_help
            exit 1
        fi
        run_app --create-job "$2"
        ;;
    --edit-job)
        if [ -z "$2" ]; then
            echo "ERROR: Job name is required"
            show_help
            exit 1
        fi
        run_app --edit-job "$2"
        ;;
    --run-job)
        if [ -z "$2" ]; then
            echo "ERROR: Job name is required"
            show_help
            exit 1
        fi
        run_app --run-job "$2"
        ;;
    --list-jobs)
        run_app --list-jobs
        ;;
    --schedule-job)
        if [ -z "$2" ]; then
            echo "ERROR: Job name is required"
            show_help
            exit 1
        fi
        
        echo "At what time would you like to run the job? (e.g., 2:00 AM = 0 2 * * *)"
        read -r cron_time
        
        schedule_job "$2" "$cron_time"
        ;;
    --list-schedules)
        list_schedules
        ;;
    *)
        # Pass all arguments to the application
        run_app "$@"
        ;;
esac

exit 0