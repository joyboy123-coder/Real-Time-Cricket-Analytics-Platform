from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="cricket_dbt_pipeline",
    start_date=datetime(2026, 7, 2),
    schedule="*/30 * * * *",   # Runs daily at 3:00 AM IST
    catchup=False,
    tags=["dbt", "cricket"]
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dags/dbt_cricket_project/dbt_project && dbt run
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dags/dbt_cricket_project/dbt_project && dbt test
        """
    )

    dbt_run >> dbt_test