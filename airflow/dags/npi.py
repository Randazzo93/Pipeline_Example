import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from transforms.ingestion import npi_pull

DBT_PROJECT_DIR = "dbt/pipeline/models"

with DAG(
    dag_id="npi_ingestion_and_dbt_daily",
    start_date=pendulum.datetime(2025, 7, 21, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["npi", "dbt"],
) as dag:
    init = EmptyOperator(task_id="init")
    done = EmptyOperator(task_id="done")
    
    # Task 1: Run the Python script to ingest NPI data
    ingest_npi_data = PythonOperator(
        task_id="ingest_npi_data",
        python_callable=npi_pull,
    )

    # Task 2: Run dbt models after the ingestion is complete
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    # Set the task dependency: The dbt run should only start after the ingestion is successful.
    init >> ingest_npi_data >> run_dbt_models >> done
