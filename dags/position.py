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
    "retry_delay": timedelta(seconds=10),
}

# Function to fetch data from the API
def fetch_api_data(**kwargs):
    
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
    query = "SELECT MEETING_KEY FROM MEETINGS_API"

    try:
        # Execute the query
        cursor = conn.cursor()
        cursor.execute(query)
        meeting_keys = [row[0] for row in cursor.fetchall()]  # Fetch session keys as a list

        # Placeholder for the final JSON results
        final_results = []
        i = 0

        # Iterate through the session keys to call the API
        for key in meeting_keys:
            api_url = f"https://api.openf1.org/v1/position?meeting_key={key}"
            response = requests.get(api_url)
            

            if response.status_code == 200:
                # Parse the JSON response and append to final results
                final_results.append(response.json())
                i += 1
                print(i)
            else:
                print(f"Failed to fetch data for session key {key}: {response.status_code}")

    finally:
        # Close the connection
        cursor.close()
        conn.close()

    data = final_results
    print(data)

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
    
    # Query to get table schema
    schemaq = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ADO_P01_GROUP4_DATABASE.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'POSITION_API'
    AND TABLE_SCHEMA = 'ASTON_MARTIN_DATA';
    """

    # Execute the query
    cursor.execute(schemaq)
    schema = cursor.fetchall()

 
    schema_dict = {col[0]: col[1] for col in schema}  # {COLUMN_NAME: DATA_TYPE}
    print(list(schema_dict.keys()))

    def generate_insert_query(table_name, schema):
        columns = ', '.join(schema.keys())
        placeholders = ', '.join(['%s'] * len(schema))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        return query

    # Example usage
    insert_query = generate_insert_query('POSITION_API', schema_dict)
    print(insert_query)

    def prepare_records(data, schema_columns):
        records = [
            tuple(record.get(column.lower(), None) for column in list(schema_columns.keys()))
            for record1 in data
            for record in record1      
        ]
        return records

    # Prepare records for executemany
    records = prepare_records(data, schema_dict)
    print(records)

    # Execute bulk insert
    cursor.executemany(insert_query, records)

    conn.commit()

    cursor.close()
    conn.close()
    os.remove(temp_file_path)  # Clean up temporary file

# DAG definition
with DAG(
    "position_to_snowflake_dag",
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