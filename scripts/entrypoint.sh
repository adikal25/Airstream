#!/bin/bash
# Script to bootstrap Airflow

# Install custom dependencies
pip install -r /opt/airflow/requirements.txt

# Initialize Airflow database
airflow db upgrade

echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start the component (passed as argument)
exec airflow "$@"