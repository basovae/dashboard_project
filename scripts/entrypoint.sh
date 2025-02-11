#!/bin/bash
echo "Initializing Airflow Database..."
airflow db init
airflow db migrate
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
echo "Starting Airflow Webserver..."
exec airflow webserver
