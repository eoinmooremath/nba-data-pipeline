#!/bin/bash

# Exit on any error
set -e

# Load environment variables
source .env

# Verify required environment variables
if [ -z "$BASE_DIR" ]; then
    echo "ERROR: BASE_DIR not set"
    exit 1
fi

# Function to log messages
log_message() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - $1"
}

log_message "Starting environment setup..."

# Create required directories
log_message "Creating directories..."
mkdir -p "${BASE_DIR}"
mkdir -p "${BASE_DIR}/scripts"
mkdir -p ~/.kaggle

# System dependencies
log_message "Installing system dependencies..."
sudo yum update -y
sudo yum install -y \
    python3-pip \
    python3-devel \
    gcc \
    unixODBC-devel \
    aws-cli

# ODBC driver
if ! rpm -qa | grep -q "msodbcsql17"; then
    log_message "Installing ODBC driver..."
    curl https://packages.microsoft.com/config/rhel/7/prod.repo | \
        sudo tee /etc/yum.repos.d/mssql-release.repo > /dev/null
    sudo ACCEPT_EULA=Y yum install -y msodbcsql17
fi

# Python virtual environment
log_message "Setting up Python virtual environment..."
if [ ! -d "${BASE_DIR}/venv" ]; then
    python3 -m venv "${BASE_DIR}/venv"
fi
source "${BASE_DIR}/venv/bin/activate"

# Install Python packages
log_message "Installing Python packages..."
pip install --upgrade pip
pip install --no-warn-script-location \
    pandas \
    pyodbc \
    kaggle \
    requests \
    beautifulsoup4

log_message "Environment setup completed successfully"