from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import boto3
import json
import pendulum
from datetime import datetime


# Define once globally (same for all tasks)
india_date = pendulum.now("Asia/Kolkata").strftime("%Y-%m-%d")


def fetch_cricket_data(**kwargs):
    api_key = "f851d11b-2280-4485-936a-b982e0e33ce1"

    url = f"https://api.cricapi.com/v1/currentMatches?apikey={api_key}&offset=0"

    response = requests.get(url)
    data = response.json()

    # Raise exception on CricAPI failure to avoid overwriting S3 with error JSON
    if data.get("status") == "failure" or "error" in data:
        reason = data.get("reason") or data.get("error") or "Unknown CricAPI error"
        raise ValueError(f"CricAPI returned a failure status: {reason}")

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket="airflowdemo1817",
        Key=f"cricket/raw/{india_date}/matches.json",
        Body=json.dumps(data)
    )


with DAG(
    dag_id="cricket_api_pipeline",
    start_date=datetime(2026, 7, 2),
    schedule="*/30 * * * *",
    catchup=False
) as dag:

    fetch_api_task = PythonOperator(
        task_id="fetch_api_task",
        python_callable=fetch_cricket_data
    )

    wait_for_file = S3KeySensor(
        task_id="wait_for_cricket_file",
        bucket_name="airflowdemo1817",
        bucket_key=f"cricket/raw/{india_date}/matches.json",
        poke_interval=60,
        timeout=600,
        mode="reschedule"

    )

    run_databricks_clean = DatabricksRunNowOperator(
        task_id="run_databricks_clean",
        databricks_conn_id="databricks_default",
        job_id=190606557968663
    )

    # MATCHES TABLE
    load_matches_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_matches_to_snowflake",
        conn_id="snowflake_default",
        sql="sql/merge_matches.sql",
        params={"india_date": india_date}
    )

    # SCORE TABLE
    load_score_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_score_to_snowflake",
        conn_id="snowflake_default",
        sql="sql/merge_score.sql",
        params={"india_date": india_date}
    )


    fetch_api_task >> wait_for_file >> run_databricks_clean >> load_matches_to_snowflake >> load_score_to_snowflake