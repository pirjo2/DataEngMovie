from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import duckdb
from datetime import datetime
import time

CSV_FILE_PATHS = {
    'User_dimension': '../movie_data/users.csv',
    'Genre_dimension': '../movie_data/tmdb.csv',
    'Keyword_dimension': '../movie_data/tmdb.csv',
    'Date_dimension': '../movie_data/holidays.csv',
    'Movie_dimension': '../movie_data/tmdb.csv',
    'Crew_dimension': '../movie_data/crew.csv',
    'Cast_dimension': '../movie_data/cast.csv',
    'Search_Cast_bridge': '../movie_data/cast.csv',
    'Search_Crew_bridge': '../movie_data/crew.csv',
    'Search_fact': '../movie_data/ratings.csv',
}

# Function to fetch row count from a DuckDB table
def get_db_row_count(table_name):
    conn = duckdb.connect(database="star_schema.db", read_only=True)
    query = f"SELECT COUNT(*) FROM {table_name}"
    result = conn.execute(query).fetchone()
    conn.close()
    time.sleep(3)
    return result[0]  # Return the count of rows

# Function to get row count from a CSV file
def get_csv_row_count(csv_file_path, get_unique):
    df = pd.read_csv(csv_file_path)
    if get_unique:
        # Actor and crewmate dimensions should only include unique gender and name paris
        return df[['name', 'gender']].drop_duplicates().shape[0]
    return len(df)

# Python function to compare the row counts
def compare_row_counts(table_name, **kwargs):
    csv_file_path = CSV_FILE_PATHS[table_name]
    get_unique = False

    if ("Crew" in table_name or "Cast" in table_name) and "bridge" not in table_name:
        get_unique = True

    # Get row count from DB and CSV
    db_row_count = get_db_row_count(table_name)
    csv_row_count = get_csv_row_count(csv_file_path, get_unique)

    # Check if the row counts match
    if db_row_count == csv_row_count:
        print(f"Row count matches for {table_name}: {db_row_count} rows.")
    else:
        print(f"Row count mismatch for {table_name}: DB ({db_row_count} rows), CSV ({csv_row_count} rows).")
        raise ValueError(f"Row count mismatch for {table_name}.")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

dag = DAG(
    'check_row_counts',
    default_args=default_args,
    description='DAG to compare row counts between CSV and Database tables',
    schedule_interval=None,
    max_active_runs=1
)

# Define tasks for each table
for table_name, csv_path in CSV_FILE_PATHS.items():
    table_validation_task = PythonOperator(
        task_id=f'check_{table_name}_row_count',
        python_callable=compare_row_counts,
        op_kwargs={'table_name': table_name},  # Pass the table_name as a keyword argument
        provide_context=True,
        dag=dag,
    )

    table_validation_task
