FROM quay.io/astronomer/astro-runtime:12.6.0

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    pip install --no-cache-dir requests snowflake-connector-python && \
    pip install --no-cache-dir mysql-connector-python
