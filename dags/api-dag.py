from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import apprise
from datetime import datetime

# Function to send notifications via Apprise to Microsoft Teams
def send_notification(message, **kwargs):
    teams_webhook_url = 'https://connectnpedu.webhook.office.com/webhookb2/6d86adbf-f30a-4761-84cb-2cd56efcdca4@cba9e115-3016-4462-a1ab-a565cba0cdf1/IncomingWebhook/22a69704773f49c5a427459d7ab53bb9/fb07c0af-703c-431b-966e-ccb9596892a7/V2g7yR_fc19jQrH4-CXX0dX_b8Dlel9J8_wpT8Z8jQlT01'
    aobj = apprise.Apprise()
    aobj.add(teams_webhook_url)
    aobj.notify(body=message, title="Airflow Notification", notify_type='info')

# Generalized function to validate data in Snowflake
def validate_data_in_snowflake(api_url, table_name, **kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    if not table_exists:
        message = f"Table '{table_name}' does not exist. Proceeding to upload data."
        kwargs['ti'].xcom_push(key=f'proceed_to_upload_{table_name}', value=True)
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        snowflake_row_count = cursor.fetchone()[0]

        response = requests.get(api_url)
        response.raise_for_status()
        api_data = response.json()

        if len(api_data) > snowflake_row_count:
            message = f"New data available for {table_name}. Proceeding to upload."
            kwargs['ti'].xcom_push(key=f'proceed_to_upload_{table_name}', value=True)
        else:
            message = f"No new data for {table_name}. Data is up-to-date."
            kwargs['ti'].xcom_push(key=f'proceed_to_upload_{table_name}', value=False)

    cursor.close()
    conn.close()
    send_notification(message, **kwargs)

# Generalized function to upload data to Snowflake
def upload_data_to_snowflake(api_url, table_name, **kwargs):
    proceed_to_upload = kwargs['ti'].xcom_pull(key=f'proceed_to_upload_{table_name}', task_ids=f'validate_data_{table_name}')
    if not proceed_to_upload:
        print(f"Skipping upload for {table_name}. Data is up-to-date.")
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    response = requests.get(api_url)
    response.raise_for_status()
    new_data = response.json()

    if not new_data:
        raise ValueError(f"No data received from {api_url}.")

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
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
    cursor.execute(create_table_query)

    cursor.execute(f"SELECT * FROM {table_name}")
    existing_data = cursor.fetchall()
    existing_keys = set([tuple(row) for row in existing_data])

    new_rows = []
    for row in new_data:
        row_tuple = tuple(row.get(column.lower(), None) for column in table_columns)
        if row_tuple not in existing_keys:
            new_rows.append(row_tuple)

    if not new_rows:
        print(f"No new rows to insert for {table_name}. Data is up-to-date.")
    else:
        for row_values in new_rows:
            placeholders = ', '.join(['%s'] * len(row_values))
            insert_query = f"INSERT INTO {table_name} ({', '.join(table_columns)}) VALUES ({placeholders})"
            cursor.execute(insert_query, row_values)

        print(f"Inserted {len(new_rows)} rows into {table_name}.")

    conn.commit()
    cursor.close()
    conn.close()
    send_notification(f"Uploaded {len(new_rows)} rows to {table_name}.", **kwargs)

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'on_success_callback': send_notification,
    'on_failure_callback': send_notification,
}

api_tasks = [
    {"api_url": "https://api.openf1.org/v1/drivers", "table_name": "drivers_api"},
    {"api_url": "https://api.openf1.org/v1/weather", "table_name": "weather_api"},
    {"api_url": "https://api.openf1.org/v1/pit", "table_name": "pit_api"},
    {"api_url": "https://api.openf1.org/v1/sessions", "table_name": "sessions_api"},
    {"api_url": "https://api.openf1.org/v1/race_control", "table_name": "race_control_api"},
    {"api_url": "https://api.openf1.org/v1/stints", "table_name": "stints_api"},
    {"api_url": "https://api.openf1.org/v1/meetings", "table_name": "meetings_api"},
]

with DAG(
    'parallel_data_upload_dag',
    default_args=default_args,
    description='DAG to upload data from multiple APIs to Snowflake in parallel',
    schedule_interval='0 0 * * *',  # Schedule the DAG to run at 12 AM daily,
    start_date=datetime(2025, 1, 4),
    catchup=False,
    tags=['snowflake', 'api', 'parallel'],
) as dag:

    validate_tasks = []
    upload_tasks = []

    for task in api_tasks:
        validate_task = PythonOperator(
            task_id=f'validate_data_{task["table_name"]}',
            python_callable=validate_data_in_snowflake,
            op_kwargs={'api_url': task["api_url"], 'table_name': task["table_name"]},
            provide_context=True,
        )
        upload_task = PythonOperator(
            task_id=f'upload_data_{task["table_name"]}',
            python_callable=upload_data_to_snowflake,
            op_kwargs={'api_url': task["api_url"], 'table_name': task["table_name"]},
            provide_context=True,
        )
        validate_task >> upload_task
        validate_tasks.append(validate_task)
        upload_tasks.append(upload_task)
