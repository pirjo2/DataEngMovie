from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Use the correct hook for your DB type (DuckDB)
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb

def query_duckdb():
    # Define your DuckDB database connection
    conn = duckdb.connect('star_schema.db')  # Make sure the correct path to the db is set

    # Define the query to fetch data from existing tables (e.g., GenreDim and RatingFact)
    query = """
        SELECT rf.FilmID, rf.Rating, gd.GenreName
        FROM RatingFact rf
        JOIN GenreDim gd ON 
        json_extract(rf.Genres, '$') LIKE '%' || gd.GenreName || '%'
        LIMIT 10;
    """

    # Execute the query
    result = conn.execute(query).fetchall()

    # Print the results (or use them as needed in further tasks)
    for row in result:
        print(f"FilmID: {row[0]}, Rating: {row[1]}, Genre: {row[2]}")

    # Close the connection
    conn.close()

# Define the DAG
with DAG(
        dag_id='duckdb_query_dag_2',
        default_args={
            'owner': 'airflow',
            'start_date': datetime(2024, 12, 4),
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=None,  # Set to None to run manually
        catchup=False,
) as dag:

    # Python task to query the database
    query_task = PythonOperator(
        task_id='query_duckdb',
        python_callable=query_duckdb,
    )

    query_task
