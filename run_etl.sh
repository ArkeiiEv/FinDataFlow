#!/bin/bash
set -e

echo "--- Starting ETL Pipeline ---"

echo "1. Stopping all containers, removing images and volumes..."
docker-compose down --rmi all --volumes

echo "2. Starting and rebuilding Docker Compose services..."
docker-compose up -d --build

echo "3. Waiting for Greenplum to initialize (giving it 30 seconds)..."
sleep 30

echo "4. Running data_extractor.py..."
docker-compose exec data_extractor python data_extractor.py

echo "5. Running data_loader.py..."
docker-compose exec data_loader python data_loader.py

echo "--- ETL Pipeline completed successfully ---"