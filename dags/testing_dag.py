from airflow.configuration import conf

# Example: Get the value of sql_alchemy_conn from the config file
sql_alchemy_conn = conf.get('core', 'sql_alchemy_conn')

print(sql_alchemy_conn)
