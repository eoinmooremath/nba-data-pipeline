#!/bin/bash

# Exit on any error
set -e

# Load environment variables
source .env

# Verify all required environment variables
for var in BASE_DIR SCRIPTS_DIR KAGGLE_USERNAME KAGGLE_KEY EC2_INSTANCE_ID; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set"
        exit 1
    fi
done

# Setup variables
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
WORK_DIR="${BASE_DIR}/nba_data_temp_${TIMESTAMP}"
VENV_DIR="${BASE_DIR}/venv"
LOG_FILE="/var/log/nba_backup.log"

# Function to log messages
log_message() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - $1"
    echo "$timestamp - $1" >> "$LOG_FILE"
    sync
}

# Error handling
handle_error() {
    log_message "ERROR: $1"
    if [ -d "$WORK_DIR" ]; then
        rm -rf "$WORK_DIR"
    fi
    exit 1
}

# Set up error handling
trap 'handle_error "Error occurred on line $LINENO"' ERR

# Create and setup log file
sudo mkdir -p /var/log
sudo touch "$LOG_FILE"
sudo chown ec2-user:ec2-user "$LOG_FILE"
> "$LOG_FILE"

# Create working directory
log_message "Creating working directory..."
mkdir -p "${WORK_DIR}/upload_dir"

# Activate virtual environment
source "${VENV_DIR}/bin/activate" || handle_error "Failed to activate virtual environment"

# Setup Kaggle credentials
log_message "Setting up Kaggle credentials..."
echo "{\"username\":\"$KAGGLE_USERNAME\",\"key\":\"$KAGGLE_KEY\"}" > ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json

# Create dataset metadata
log_message "Creating dataset metadata..."
cat > "${WORK_DIR}/upload_dir/dataset-metadata.json" << EOL
{
    "title": "Historical NBA Data and Player Box Scores",
    "id": "${KAGGLE_USERNAME}/historical-nba-data-and-player-box-scores",
    "licenses": [{"name": "CC0-1.0"}]
}
EOL

# Copy and run Python scripts
log_message "=== STARTING DATA EXPORT PHASE ==="
cd "${WORK_DIR}"

cp "${SCRIPTS_DIR}/export_tables.py" "${WORK_DIR}/" || handle_error "Failed to copy export_tables.py"
cp "${SCRIPTS_DIR}/create_sql_dump.py" "${WORK_DIR}/" || handle_error "Failed to copy create_sql_dump.py"

log_message "=== STARTING CSV EXPORT ==="
python3 export_tables.py || handle_error "Failed to export CSVs"
log_message "=== CSV EXPORT COMPLETED ==="

log_message "=== STARTING SQL DUMP CREATION ==="
python3 create_sql_dump.py || handle_error "Failed to create SQL dump"
log_message "=== SQL DUMP CREATION COMPLETED ==="

# Move files to upload directory
log_message "Moving files to upload directory..."
for file in *.csv NBA_Database.sql; do
    if [ -f "$file" ]; then
        mv "$file" "${WORK_DIR}/upload_dir/" || handle_error "Failed to move $file"
    fi
done

# Kaggle upload
log_message "=== STARTING KAGGLE UPLOAD PHASE ==="
cd "${WORK_DIR}/upload_dir"
DATASET_NAME="${KAGGLE_USERNAME}/historical-nba-data-and-player-box-scores"
TODAY=$(date +%Y%m%d)

if kaggle datasets status "$DATASET_NAME" 2>/dev/null; then
    log_message "Dataset exists, creating new version..."
    kaggle datasets version -m "Update $TODAY - Full backup" -p "." || handle_error "Failed to upload to Kaggle"
else
    log_message "Creating new dataset..."
    kaggle datasets create -p "." || handle_error "Failed to create dataset"
fi

# Cleanup
log_message "=== STARTING CLEANUP PHASE ==="
cd "${BASE_DIR}"
find "${BASE_DIR}" -type f -name "*.csv" -delete
find "${BASE_DIR}" -type f -name "*.sql" -delete
rm -rf "$WORK_DIR"

log_message "=== BACKUP COMPLETED SUCCESSFULLY ==="

log_message "=== INITIATING EC2 INSTANCE SHUTDOWN ==="
aws ec2 stop-instances --instance-ids "$EC2_INSTANCE_ID" || log_message "WARNING: Failed to stop EC2 instance"
sleep 10