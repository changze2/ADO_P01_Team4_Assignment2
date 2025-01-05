from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import apprise
from datetime import datetime

# Function to send notifications via Apprise to Microsoft Teams
def send_notification(message, **kwargs):
    # Replace with your Microsoft Teams webhook URL
    teams_webhook_url = 'https://connectnpedu.webhook.office.com/webhookb2/6d86adbf-f30a-4761-84cb-2cd56efcdca4@cba9e115-3016-4462-a1ab-a565cba0cdf1/IncomingWebhook/22a69704773f49c5a427459d7ab53bb9/fb07c0af-703c-431b-966e-ccb9596892a7/V2g7yR_fc19jQrH4-CXX0dX_b8Dlel9J8_wpT8Z8jQlT01'
    
    # Initialize Apprise
    aobj = apprise.Apprise()
    aobj.add(teams_webhook_url)
    
    # Send the notification
    aobj.notify(
        body=message,
        title="Airflow Notification",
        notify_type='info'
    )

# Function to validate data in Snowflake
def validate_data_in_snowflake(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    table_name = 'drivers_api'
    api_url = 'https://api.openf1.org/v1/drivers'

    # Check if the table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    if not table_exists:
        message = f"Table '{table_name}' does not exist. Proceeding to upload data."
        kwargs['ti'].xcom_push(key='proceed_to_upload', value=True)
    else:
        # Fetch current row count in Snowflake
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        snowflake_row_count = cursor.fetchone()[0]

        # Fetch data from the API
        response = requests.get(api_url)
        response.raise_for_status()
        api_data = response.json()

        if len(api_data) > snowflake_row_count:
            message = "New driver data available. Proceeding to upload data."
            kwargs['ti'].xcom_push(key='proceed_to_upload', value=True)
        else:
            message = "No new driver data to insert. Data is already up-to-date."
            kwargs['ti'].xcom_push(key='proceed_to_upload', value=False)

    cursor.close()
    conn.close()

    # Send notification to Microsoft Teams
    send_notification(message, **kwargs)

# Function to upload data to Snowflake
def upload_data_to_snowflake(**kwargs):
    # Check if the upload task should proceed
    proceed_to_upload = kwargs['ti'].xcom_pull(key='proceed_to_upload', task_ids='validate_data_in_snowflake')
    if not proceed_to_upload:
        print("Skipping data upload as the dataset is already up-to-date.")
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    table_name = 'drivers_api'
    api_url = 'https://api.openf1.org/v1/drivers'

    # Fetch data from the API
    response = requests.get(api_url)
    response.raise_for_status()
    new_data = response.json()

    if not new_data:
        raise ValueError("No data received from the API.")

    # Extract schema dynamically
    table_columns = []
    table_schema = ''

    for column in new_data[0]:
        if isinstance(new_data[0][column], dict):
            for sub_column in new_data[0][column]:
                col_name = f"{column}_{sub_column}".upper()
                table_columns.append(col_name)
                table_schema += f"{col_name} STRING, "
        else:
            col_name = column.upper()
            table_columns.append(col_name)
            table_schema += f"{col_name} STRING, "

    table_schema = table_schema.rstrip(', ')

    # Create table if it doesn't exist
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
    cursor.execute(create_table_query)

    # Fetch existing data from Snowflake
    cursor.execute(f"SELECT * FROM {table_name}")
    existing_data = cursor.fetchall()
    existing_keys = set([tuple(row) for row in existing_data])  # Use a unique identifier (e.g., primary key)

    # Find new rows to upload
    new_rows = []
    for row in new_data:
        row_tuple = tuple(
            row.get(column.lower(), None) for column in table_columns
        )  # Create a tuple of row values
        if row_tuple not in existing_keys:
            new_rows.append(row_tuple)

    if not new_rows:
        print("No new rows to insert. Data is already up-to-date.")
    else:
        # Insert only new rows
        for row_values in new_rows:
            placeholders = ', '.join(['%s'] * len(row_values))
            insert_query = f"INSERT INTO {table_name} ({', '.join(table_columns)}) VALUES ({placeholders})"
            cursor.execute(insert_query, row_values)

        print(f"Inserted {len(new_rows)} new rows into the {table_name} table.")

    conn.commit()
    cursor.close()
    conn.close()

    # Send notification to Microsoft Teams
    send_notification(f"Uploaded {len(new_rows)} new rows to Snowflake.", **kwargs)

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'on_success_callback': send_notification,
    'on_failure_callback': send_notification,
}

with DAG(
    'python_to_snowflake_dag',
    default_args=default_args,
    description='DAG to validate and upload data from Python to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 4),
    catchup=False,
    tags=['snowflake', 'api', 'example'],
) as dag:

    # Task 1: Validate data in Snowflake
    validate_data_task = PythonOperator(
        task_id='validate_data_in_snowflake',
        python_callable=validate_data_in_snowflake,
        provide_context=True,
        on_success_callback=send_notification,
        on_failure_callback=send_notification
    )

    # Task 2: Upload data to Snowflake
    upload_task = PythonOperator(
        task_id='upload_data_to_snowflake',
        python_callable=upload_data_to_snowflake,
        provide_context=True,
        on_success_callback=send_notification,
        on_failure_callback=send_notification
    )

    # Define task dependencies
    validate_data_task >> upload_task
