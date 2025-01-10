from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import tempfile
import os
from snowflake.connector import connect

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Function to fetch data from the API
def fetch_api_data(**kwargs):
    api_url = "https://api.openf1.org/v1/drivers"
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error for bad responses
    data = response.json()

    # Save data to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as temp_file:
        json.dump(data, temp_file)
        temp_file.close()
        kwargs["ti"].xcom_push(key="temp_file_path", value=temp_file.name)

# Function to load data into Snowflake
def load_to_snowflake(**kwargs):
    temp_file_path = kwargs["ti"].xcom_pull(key="temp_file_path")
    with open(temp_file_path, "r") as file:
        data = json.load(file)
    
    # Snowflake connection
    conn = connect(
        user="GOPHER",
        password="OFahH1u3",
        account="BAB83824",
        warehouse="ADO_P01_GROUP4_WAREHOUSE",
        database="ADO_P01_GROUP4_DATABASE",
        schema="ASTON_MARTIN_DATA",
        role='ADO_P01_GROUP4_DEVELOPER'
    )
    cursor = conn.cursor()
    
    # Insert data into the table
    for record in data:
         
        values = (
        record["broadcast_name"],
        record["country_code"],
        record["driver_number"],  # Assuming DRIVER_NUMBER is an integer, leave it as is
        record["first_name"],
        record["full_name"],
        record["headshot_url"],
        record["last_name"],
        record["meeting_key"],
        record["name_acronym"],
        record["session_key"],
        record["team_colour"],
        record["team_name"]
        )

        cursor.execute("""
            INSERT INTO DRIVERS_API
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)

    cursor.close()
    conn.close()
    os.remove(temp_file_path)  # Clean up temporary file

# DAG definition
with DAG(
    "api_to_snowflake_dag",
    default_args=default_args,
    description="Extract data from an API and load it into Snowflake",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to extract data
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=fetch_api_data,
    )

    # Task to load data
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_to_snowflake,
    )

    extract_task >> load_task