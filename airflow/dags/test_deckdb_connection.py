from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime, timedelta
import logging

# Function to test the DuckDB connection
def test_duckdb_connection():
    try:
        # Connect to your DuckDB database
        conn = duckdb.connect(database='star_schema.db')

        # Run a simple query to check if the connection is successful
        result = conn.execute("SELECT * from GenreDim LIMIT 10;").fetchall()

        logging.info(f"Connection test successful: {result}")

        conn.close()
    except Exception as e:
        logging.error(f"Error connecting to DuckDB: {e}")
        raise  # Reraise the exception to fail the task

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 4),  # Update with your desired start date
}

# Define the DAG
dag = DAG(
    'test_duckdb_connection_dag',
    default_args=default_args,
    description='A DAG to test DuckDB connection',
    schedule_interval=None,  # No schedule, will run manually
    catchup=False,
)

# PythonOperator to run the connection test
test_connection_task = PythonOperator(
    task_id='test_duckdb_connection',
    python_callable=test_duckdb_connection,
    dag=dag,
)

# Set the task in the DAG
test_connection_task
