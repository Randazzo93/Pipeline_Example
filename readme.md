# Data Pipeline Example

This repository demonstrates a simple, modern data pipeline that extracts data from a public API, loads it into a data warehouse, and transforms it for analytics.

## Pipeline Architecture

The pipeline follows an Extract, Load, Transform (ELT) pattern.

```
  NPI Registry API
        |
        v
+------------------+
| Airflow (Python) | (Extract & Load)
_------------------+
        |
        v
+----------------------+
| BigQuery (Raw Table) |
|   (healthcare.npi)   |
----------------------+
        |
        v
+------------------+
|   dbt (SQL)      | (Transform)
------------------+
        |
        v
+---------------------------+
| BigQuery (Transformed Tbl)|
| (healthcare.npi_flattened)|
---------------------------+
```

## Core Technologies

*   **Orchestration**: [Apache Airflow](https://airflow.apache.org/) is used to schedule and run the data pipeline tasks. The environment is containerized using the provided `docker-compose.yaml`.
*   **Transformation**: dbt (Data Build Tool) handles the SQL-based transformation of the raw data into a clean, analytics-ready format.
*   **Data Warehouse**: Google BigQuery serves as the data warehouse for storing both the raw, semi-structured data and the final transformed data.
*   **Dependency Management**: Poetry is used to manage Python dependencies for the project.

## Pipeline Steps

### 1. Extract & Load

This step is performed by a Python script, intended to be run as an Airflow task.

*   **Script**: `/Users/tomrandazzo/Repos/Pipeline_Example/airflow/transforms/npi.py`
*   **Source**: It calls the NPI Registry API to fetch data for a specific query: Neurologists in Elk Grove Village, IL.
*   **Loading**: The script uses the `google-cloud-bigquery` library to load the raw JSON results directly into a BigQuery table named `healthcare.npi`.
*   **Load Strategy**: The table is overwritten on each run (`WRITE_TRUNCATE`), ensuring the data is always fresh based on the last API call. The schema is auto-detected by BigQuery from the JSON structure.

### 2. Transform

This step uses dbt to clean and remodel the raw data.

*   **Model**: `/Users/tomrandazzo/Repos/Pipeline_Example/dbt/pipeline/models/npi_flattened.sql` (as seen in `manifest.json`)
*   **Source Table**: It reads from the raw `healthcare.npi` table in BigQuery.
*   **Logic**: The raw data from the API contains nested arrays for addresses, taxonomies, and other identifiers. The dbt model uses `LEFT JOIN UNNEST(...)` to flatten these arrays, creating a wide, denormalized table where each row represents a unique combination of a provider and one of their addresses, taxonomies, or identifiers.
*   **Destination Table**: The transformed data is materialized as a new table named `healthcare.npi_flattened`.

## How to Run

### Prerequisites

1.  Docker and Docker Compose
2.  Google Cloud SDK authenticated with a user or service account that has BigQuery access.
3.  dbt Core installed locally.
4.  A `profiles.yml` file configured for your BigQuery project. See dbt BigQuery Profile setup.

### Execution

1.  **Start Airflow (for EL step)**:
    In a real-world scenario, you would have a DAG that orchestrates these steps. For manual execution:
    ```bash
    # Navigate to the airflow directory
    cd airflow

    # Start the Airflow services
    docker-compose up -d
    ```
    You would then trigger the DAG responsible for running the `npi_pull` task.

2.  **Run dbt Transformation**:
    After the raw data has been loaded into the `healthcare.npi` table, you can run the dbt transformation.
    ```bash
    # Navigate to the dbt project directory
    cd dbt/pipeline

    # Run the dbt model
    dbt run --select npi_flattened
    ```
    This will create the `npi_flattened` table in your BigQuery dataset.

## Repository Structure

```
/
├── airflow/            # Contains Airflow configuration and tasks
│   ├── transforms/     # Python scripts for EL tasks
│   └── docker-compose.yaml # Local Airflow environment setup
├── dbt/                # Contains the dbt project for transformations
│   └── pipeline/
└── poetry.lock         # Python project dependencies
```