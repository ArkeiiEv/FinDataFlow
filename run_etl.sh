#!/bin/bash

echo "--- Starting ETL Pipeline with Apache Airflow ---"

docker compose down --rmi all --volumes

echo "Starting database services..."
docker compose up -d airflow_postgres greenplum

echo "Waiting for Airflow's PostgreSQL to be ready (max 60 seconds)..."
for i in $(seq 1 12); do
  docker exec airflow_postgres pg_isready -U airflow -d airflow > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Airflow's PostgreSQL is ready."
    break
  fi
  echo "Waiting... ($i/12)"
  sleep 5
done

echo "Waiting for Greenplum to be ready (max 60 seconds)..."
for i in $(seq 1 12); do
  docker exec greenplum pg_isready -U ${DB_USER} -d ${DB_NAME} > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Greenplum is ready."
    break
  fi
  echo "Waiting... ($i/12)"
  sleep 5
done

echo "Running Airflow database migrations and user creation..."
docker compose --profile init up --build --abort-on-container-exit

echo "Starting Airflow webserver, scheduler, and worker..."
docker compose --profile webserver --profile scheduler --profile worker up --build -d

echo "--- Airflow should now be accessible at http://localhost:8080 ---"
echo "Default credentials: admin / airflow"
