#!/bin/bash

echo "--- Starting ETL Pipeline with Apache Airflow ---"

echo "Performing cleanup of previous Docker resources..."
docker stop $(docker ps -a -q --filter ancestor=finance-etl-airflow-webserver --filter ancestor=finance-etl-airflow-scheduler --filter ancestor=finance-etl-airflow-worker --filter ancestor=andruche/greenplum --filter ancestor=postgres) 2>/dev/null || true
docker rm $(docker ps -a -q --filter ancestor=finance-etl-airflow-webserver --filter ancestor=finance-etl-airflow-scheduler --filter ancestor=finance-etl-airflow-worker --filter ancestor=andruche/greenplum --filter ancestor=postgres) 2>/dev/null || true

docker compose down --rmi all --volumes
docker network rm finance_etl_network || true
docker network prune -f || true
docker container prune -f || true
docker image prune -f --filter "label=org.opencontainers.image.source=https://github.com/apache/airflow" || true
sleep 1

echo "Creating custom Docker network: finance_etl_network..."
docker network create finance_etl_network || true
sleep 3

echo "Starting database services..."
docker compose up -d airflow_postgres greenplum

echo "Waiting for Airflow's PostgreSQL to be ready (max 60 seconds)..."
for i in $(seq 1 12); do
  if docker exec finance-etl-airflow_postgres-1 pg_isready -U airflow -d airflow > /dev/null 2>&1; then
    echo "Airflow's PostgreSQL is ready."
    break
  fi
  echo "Waiting for Airflow's PostgreSQL... ($i/12)"
  sleep 5
done

echo "Waiting for Greenplum to be ready (max 60 seconds)..."
for i in $(seq 1 12); do
  if docker exec greenplum pg_isready -U ${DB_USER} -d ${DB_NAME} > /dev/null 2>&1; then
    echo "Greenplum is ready."
    break
  fi
  echo "Waiting for Greenplum... ($i/12)"
  sleep 5
done

echo "Running Airflow database migrations and user creation..."
docker compose run --rm --build airflow-init airflow db migrate
docker compose run --rm --build airflow-init airflow users create --username admin --password admin --firstname Airflow --lastname User --role Admin --email admin@example.com || true

sleep 15

echo "Starting Airflow webserver and scheduler..."
docker compose --profile webserver --profile scheduler up --build -d

echo "--- Airflow should now be accessible at http://localhost:8080 ---"
echo "Default credentials: admin / airflow"