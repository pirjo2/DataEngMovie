from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb

# Path to your DuckDB database
DUCKDB_PATH = 'star_schema.db'

# List of tables to truncate
TABLES_TO_TRUNCATE = [
    "Cast_dimension",
    "Crew_dimension",
    "Date_dimension",
    "Genre_dimension",
    "Keyword_dimension",
    "Movie_dimension",
    "Search_Cast_bridge",
    "Search_Crew_bridge",
    "Search_fact",
    "User_dimension",
]

def truncate_duckdb_tables():
    """
    Connect to the DuckDB database and truncate the specified tables.
    """
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(DUCKDB_PATH)
        print("Connected to DuckDB.")

        # Truncate each table
        for table in TABLES_TO_TRUNCATE:
            conn.execute(f"DELETE FROM {table};")
            print(f"Truncated table: {table}")

        print("All specified tables truncated successfully!")

    except Exception as e:
        print(f"Error truncating tables: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("DuckDB connection closed.")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

with DAG(
        dag_id='truncate_duckdb_tables',
        default_args=default_args,
        schedule_interval=None,  # Trigger manually
        catchup=False,
        tags=['maintenance'],
) as dag:

    truncate_task = PythonOperator(
        task_id='truncate_tables_task',
        python_callable=truncate_duckdb_tables,
    )

truncate_task
