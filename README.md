# Financial ETL Pipeline with Apache Airflow

## Project Description

This project provides a comprehensive ETL (Extract, Transform, Load) pipeline designed for the automated collection, processing, and loading of financial data. It leverages **Apache Airflow** for workflow orchestration, **Greenplum Database** as a scalable data warehouse, and **PostgreSQL** for Airflow's metadata.

The pipeline is designed to extract historical stock price data (e.g., from the Alpha Vantage API), save it in an intermediate format (CSV), and then load it into the Greenplum analytical database for further analysis.

## Features

* **Automated Data Extraction:** Periodic collection of financial data from external sources.
* **Flexible Transformation:** Capability to add data transformation steps before loading.
* **Reliable Loading:** Loading of processed data into Greenplum Database.
* **Airflow-based Orchestration:** Centralized management, monitoring, and scheduling of all ETL tasks through an intuitive web interface.
* **Docker Containerization:** All components are deployed in Docker containers for portability, isolation, and ease of deployment.
* **Resilience:** Built-in mechanisms for database readiness checks and error handling.

## Getting Started

To run this project, you will need Docker and Docker Compose installed.

### 1. Prerequisites

* **Docker** and **Docker Compose** on your Linux machine.
* **`psql` client** (for verifying data in Greenplum):
    ```bash
    sudo apt-get update && sudo apt-get install postgresql-client
    ```
    
### 2. Configuration

1.  **`.env` file**: Create or update your `.env` file in the project root. This file holds sensitive information like database credentials and API keys.
    * **Configure Greenplum credentials:** Set values for `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT`.
    * **Set Alpha Vantage API key and symbol:** Provide your `API_KEY` and `SYMBOL`.
    * **Define Airflow User ID:** Ensure `AIRFLOW_UID` matches your host user's UID (you can get it with `id -u`).

2.  **`Dockerfile.airflow`**: Located in the `airflow/` directory. This Dockerfile extends the official Airflow image and installs necessary Python dependencies for your DAGs (e.g., `pandas`, `psycopg2-binary`).

3.  **`docker-compose.yml`**: Located in the project root. This file defines all services (Greenplum, PostgreSQL, Airflow components), their configurations, network settings, and volume mounts. It also sets resource limits (`mem_limit`) for Airflow services for better stability.

4.  **`run_etl.sh`**: This script is located in the project root and orchestrates the entire Docker stack startup process, including cleanup, network creation, database readiness checks, Airflow migrations, and service initiation. Ensure it has execute permissions (`chmod +x run_etl.sh`).

### 3. Setup and Running the Pipeline

1.  **Initial Setup and Permissions:**
    Navigate to your project root directory in the terminal:
    ```bash
    cd ~/EDU/finance-etl/
    ```
    Ensure the `raw_data` and `logs` directories exist and have the correct permissions. This is crucial for preventing `Permission denied` errors during data extraction and logging.
    ```bash
    mkdir -p raw_data logs
    sudo chown -R 1000:$(id -g) ./raw_data
    sudo chmod -R u+rwx,g+rwx,o+rwx ./raw_data
    sudo chown -R 1000:$(id -g) ./logs
    sudo chmod -R u+rwx,g+rwx,o+rwx ./logs
    ```
    *(Note: `1000` is the `AIRFLOW_UID` set in `.env`. `$(id -g)` ensures the group matches your current user's primary group.)*
    Grant execute permissions to the `run_etl.sh` script:
    ```bash
    chmod +x run_etl.sh
    ```

2.  **Running the ETL Pipeline:**
    Execute the main script to set up and start all services:
    ```bash
    ./run_etl.sh
    ```
    This script will perform cleanup, create the Docker network, start database services, run Airflow database migrations and user creation, and finally start the Airflow webserver and scheduler.

### 4. Accessing Airflow UI

After the `run_etl.sh` script completes, you should see the message:
--- Airflow should now be accessible at http://localhost:8080 ---
Default credentials: admin / airflow


**Important:** Even after this message, the Airflow webserver might need an additional **30-60 seconds** to fully initialize and become responsive. If you encounter a "connection reset" error in your browser, wait a bit longer and refresh the page.

Open your web browser and navigate to:

http://localhost:8080


Log in with the default credentials:
* **Username:** `admin`
* **Password:** `admin`

In the Airflow UI, activate the `financial-etl` DAG and manually trigger a run.

### 5. Verifying Data in Greenplum

Once the `financial-etl` DAG has completed successfully (all tasks are green in the Airflow UI Graph View), you can verify the data loaded into Greenplum.

1.  Open a new terminal.
2.  Connect to the Greenplum database using `psql`:
    ```bash
    psql -h localhost -p 5432 -U gpadmin -d postgres
    ```
    When prompted for the password, enter `12345678`.
3.  Once connected, you can check for the `stock_prices` table in the `staging` schema and query its data:

    * List tables in the `staging` schema:
        ```sql
        \dt staging.*
        ```
        You should see `staging | stock_prices | table | gpadmin`.

    * View the first 10 rows of the `stock_prices` table:
        ```sql
        SELECT * FROM staging.stock_prices LIMIT 10;
        ```

    * Count the number of rows in the table:
        ```sql
        SELECT COUNT(*) FROM staging.stock_prices;
        ```
4.  To exit `psql`, type `\q` and press Enter.

### 6. Troubleshooting Common Issues

* **`ModuleNotFoundError: No module named 'airflow.__main__'`:**
    * Ensure your `Dockerfile.airflow` is clean and only contains `FROM` and `RUN pip install` commands, without any `echo` or `USER` commands that might interfere with Airflow's entrypoint.
    * Perform a full Docker cleanup (`docker compose down -v --rmi all && sudo systemctl restart docker`).
* **`PermissionError: [Errno 13] Permission denied: '/data/raw/stock_prices_YYYYMMDD.csv'`:**
    * This indicates that the user inside the Docker container (mapped to `AIRFLOW_UID=1000`) does not have write permissions to the `./raw_data` directory on your host.
    * Re-run the `sudo chown -R` and `sudo chmod -R` commands from Section 4.1.
* **"Connection reset" in browser / Airflow UI not loading:**
    * **Wait longer:** The Airflow webserver needs time to fully initialize. Wait at least 30-60 seconds after `run_etl.sh` finishes before trying to access `http://localhost:8080`.
    * **Port Conflict:** Verify no other process is using port 8080 on your host: `sudo netstat -tulnp | grep 8080`. If so, either stop the conflicting process or change the host port mapping in `docker-compose.yml` (e.g., `8081:8080`).
    * **Firewall:** Temporarily disable your host's firewall (`sudo ufw disable`) for testing. If it resolves the issue, add a permanent rule: `sudo ufw allow 8080/tcp`.
    * **Resource Limits:** Ensure Docker has sufficient memory and CPU allocated (e.g., 4GB+ RAM, 2+ CPUs). Adjust `mem_limit` in `docker-compose.yml` if necessary.
* **`Did not find any relations.` in `psql`:**
    * Verify that all DAG tasks in the Airflow UI have completed successfully (green status). If any failed, check their logs for specific errors.
    * Ensure your DAG is configured to load data into the `public` or `staging` schema (or whichever schema you are checking). The `\dt staging.*` command is useful for checking specific schemas.

