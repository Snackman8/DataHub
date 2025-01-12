#!/bin/bash

readonly PROVIDER_NAME="example_provider"

# Exit immediately if a command exits with a non-zero status
set -e

# Check if the script is run as root (via sudo or as root user)
if [ "$(id -u)" -ne 0 ]; then
  echo "Error: This script must be run with sudo or as root." >&2
  exit 1
fi

# Get the current working directory
current_dir=$(pwd)

# install the module
mkdir -p /srv/${PROVIDER_NAME}
cp -r ${current_dir}/../src /srv/${PROVIDER_NAME}/
cd /srv/${PROVIDER_NAME}
virtualenv venv
source /srv/${PROVIDER_NAME}/venv/bin/activate
pip install -r ${current_dir}/../requirements.txt

# install the proxy file
mkdir -p /etc/apache2/proxy-configs
cp ${current_dir}/proxy_${PROVIDER_NAME}.conf /etc/apache2/proxy-configs
systemctl restart apache2

# install the supervisor file
cp ${current_dir}/supervisor_${PROVIDER_NAME}.conf /etc/supervisor/conf.d/
supervisorctl reload
