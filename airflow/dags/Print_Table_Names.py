from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime, timedelta

# Function to fetch and print table names
def print_table_names():
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(database="star_schema.db")

        # Fetch all table names
        result = conn.execute("SHOW TABLES;").fetchall()

        # Print table names
        table_names = [row[0] for row in result]
        print("Tables in star_schema.db:")
        for table_name in table_names:
            print(f"- {table_name}")

        # Close the connection
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Error fetching table names: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 9),
}

# Define the DAG
with DAG(
        'print_star_schema_tables',
        default_args=default_args,
        description='A DAG to print all table names in the star_schema.db DuckDB database',
        schedule_interval=None,  # No schedule, run manually
        catchup=False,
) as dag:

    # Define the PythonOperator task
    fetch_table_names_task = PythonOperator(
        task_id='print_table_names',
        python_callable=print_table_names,
    )
