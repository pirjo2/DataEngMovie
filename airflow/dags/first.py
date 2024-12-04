from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging
from datetime import datetime, timedelta

# Function to test DuckDB connection and query for all table names
def test_duckdb_connection():
    try:
        # Connect to DuckDB
        conn = duckdb.connect(database='star_schema.db')

        # Query to list all tables in the schema
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main';  -- Adjust schema name if necessary
        """

        # Execute the query to fetch all table names
        result = conn.execute(query).fetchall()

        # Log the result
        logging.info("Tables in DuckDB schema:")
        for row in result:
            logging.info(row[0])  # Print each table name

        # Close the connection
        conn.close()

    except Exception as e:
        logging.error(f"Error connecting to DuckDB or executing query: {e}")
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
    'firts',
    default_args=default_args,
    description='A DAG to test DuckDB connection and query tables',
    schedule_interval=None,  # No schedule, will run manually
    catchup=False,
)

# PythonOperator to run the connection test and query
test_connection_task = PythonOperator(
    task_id='test_duckdb_connection',
    python_callable=test_duckdb_connection,
    dag=dag,
)

# Set the task in the DAG
test_connection_task
