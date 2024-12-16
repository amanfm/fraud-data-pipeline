#!/bin/bash

set -x  # Enable debugging
apt-get update

# Define the directory and script to run
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Script directory: $SCRIPT_DIR"

pip install -r requirements.txt
rm -rf logs
mkdir logs

set +x  # Disable debugging