# Tennis Data Analytics

This project is a complete data engineering pipeline focused on extracting, processing, and analyzing historical and incremental data of ATP Tennis matches and rankings. It uses **Apache Airflow** for orchestration, **PostgreSQL** for the analytical database (Data Warehouse), **Minio (S3)** as a Data Lake, and **PySpark** / **Pandas** for processing.

---

## 🛠 Prerequisites

To run this project locally, ensure you have the following components installed:
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- [Python 3.11+](https://www.python.org/)
- Account and terminal access (Git Bash, WSL, or PowerShell)

---

## 🚀 How to Run the Project (End-to-End)

Follow these steps strictly after cloning the repository for the first time.

### 1. Configure Environment Variables and Credentials
The project requires database credentials configurations.

1. Create the `.env` file in the root of the project:
   ```bash
   cp .env.example .env
   ```
   *Fill in the `.env` file with your sensitive information, if necessary.*

2. Open the `docker-compose.yml` file and replace all **placeholders** (`<<username>>`, `<<password>>`, `<<database>>`) with the credentials you want to use for the Main Database (`postgres_db`) and for Minio (`minio_s3`).

### 2. Download Local Historical Data (Initial Load)
The project has two independent scripts that do not rely on Airflow. They are responsible for scraping the internet and pulling all historical Tennis data since 1968, saving them in an organized way in your computer's local folder (`raw/`).

Open your terminal in the root folder and install the dependencies if you haven't already:
```bash
pip install -r requirements.txt
playwright install chromium
```

Then, execute the two scripts:
```bash
python bot/init_historical_matches.py
python bot/init_historical_rankings.py
```
*This will populate the `raw/historical` and `raw/incremental` folders with all the necessary CSVs to feed the Data Lake.*

### 3. Spin Up the Containers (Airflow, Postgres, Minio)
With the historical data in hand and the credentials configured, bring up the entire Docker infrastructure:

```bash
docker-compose up -d --build
```
This will start the following services:
- **Airflow Webserver and Scheduler** (Accessible at `http://localhost:8080`)
- **Minio S3** (Accessible at `http://localhost:9001`)
- **Postgres DW** (Accessible on port `5432`)
- **Postgres Airflow Backend**

---

## ⚙ Mandatory Apache Airflow Configurations

As soon as Airflow is up, access the UI (`http://localhost:8080`) with the default credentials set in `docker-compose.yml` in the `airflow-init` service.

For your extraction and ingestion DAGs to work properly, you MUST configure the following **Variables** and **Connections**:

### 1. Variables (Admin -> Variables)
Your python scripts (e.g., `match_extractor.py`) expect to receive S3 credentials as a dictionary (`conn_vars`). If your DAG invokes them and passes a Variable, make sure to create the respective **Variable** (e.g., `minio_credentials`) in JSON format containing:
```json
{
  "bucket_endpoint": "http://minio_s3:9000",
  "bucket_access_key": "your_minio_user",
  "bucket_secret_key": "your_minio_password",
  "bucket_name": "tennis-data"
}
```

### 2. Connections (Admin -> Connections)
You also need to create connections to allow Airflow to write data to the systems.

- **Minio / S3 Connection**
  - **Connection ID:** `minio_s3_conn` (or the name used in your DAGs)
  - **Connection Type:** `Amazon Web Services` or `S3`
  - **Login:** *your minio user (MINIO_ROOT_USER)*
  - **Password:** *your minio password (MINIO_ROOT_PASSWORD)*
  - **Extra:** `{"endpoint_url": "http://minio_s3:9000"}`

- **PostgreSQL Connection (Main Data Warehouse)**
  - **Connection ID:** `postgres_dw` (or the exact name used in your DAG/Pyspark)
  - **Connection Type:** `Postgres`
  - **Host:** `postgres_db` (service name in docker-compose)
  - **Schema:** *database name you chose*
  - **Login:** *your postgres username*
  - **Password:** *your postgres password*
  - **Port:** `5432`

---

## ▶ Uploading Files and Running the Pipelines

With everything configured:
1. **Minio:** Access the Minio dashboard (`http://localhost:9001`), create the main bucket (e.g., `tennis-data`), and manually upload the files that were generated in the local `raw/` folder from step 2.
2. **Airflow:** Turn on your DAGs (`DAGs Toggle On`). Airflow will take care of reading from Minio, processing in PySpark, and sending the refined result to the PostgreSQL Database.

## 🧽 Shutting Down the Project
To bring down the infrastructure without deleting database volumes and local files:
```bash
docker-compose stop
```
To remove everything and reset the environment completely:
```bash
docker-compose down -v
```

---

## 🙌 Acknowledgements

A special thanks to **Tennis My Life** for providing the comprehensive historical data that makes this project possible!
- **Match Data:** Extracted from [stats.tennismylife.org](https://stats.tennismylife.org/)
- **Historical Rankings:** Extracted from their incredible open-source repository: [Tennismylife/TML-Rankings-Database](https://github.com/Tennismylife/TML-Rankings-Database)