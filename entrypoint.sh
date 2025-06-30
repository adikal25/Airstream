echo '#!/bin/bash
echo "Starting Airflow Webserver..."
exec airflow webserver' > ./script/entrypoint.sh