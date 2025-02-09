import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Profile configuration for Snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "ADO_P01_GROUP4_DATABASE", "schema": "ASTON_MARTIN_DATA"},
    )
)

# Define the Airflow DAG
dag = DAG(
    "dbt_dag_with_git_pull",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 12, 25),
    },
    schedule_interval="0 1 * * *",
    catchup=False,
)

# Task to pull the latest changes from the main-dbt branch
pull_dbt_branch = BashOperator(
    task_id="pull_dbt_branch",
    bash_command="""
    cd /usr/local/airflow/dags/dbt_testing &&
    git fetch origin main-dbt &&
    git checkout main-dbt &&
    git pull origin main-dbt
    """,
    dag=dag,
)

# dbt DAG task configuration
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt_testing"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 12, 25),
    catchup=False,
    dag_id="dbt_dag",
)

# Task sequence: Pull the branch first, then run dbt
pull_dbt_branch >> dbt_snowflake_dag
