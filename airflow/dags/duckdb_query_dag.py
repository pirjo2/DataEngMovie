from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb

# DuckDB query function
def query_duckdb():
    conn = duckdb.connect('star_schema.db')  # Ensure path matches mounted volume
    result = conn.execute('SELECT * FROM my_table LIMIT 10').fetchall()
    print("Query Results:", result)

# Define DAG
with DAG(
        dag_id='duckdb_query_dag',
        start_date=datetime(2023, 12, 1),
        schedule_interval=None,  # Set to None for manual runs
        catchup=False
) as dag:
    query_task = PythonOperator(
        task_id='query_duckdb',
        python_callable=query_duckdb
    )

    query_task
