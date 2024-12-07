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
        #query = """
        #    SELECT table_name
        #    FROM information_schema.tables
        #    WHERE table_schema = 'main';  -- Adjust schema name if necessary
        #"""
        query = """
            CREATE TABLE IF NOT EXISTS GenreDim (
                    GenreID INTEGER PRIMARY KEY,
                    GenreName TEXT
                );
            CREATE TABLE IF NOT EXISTS RatingFact (
                    FilmID INTEGER PRIMARY KEY,
                    Rating FLOAT,
                    Genres JSON
                );
            DELETE FROM GenreDim;
            DELETE FROM RatingFact;
            INSERT INTO GenreDim (GenreID, GenreName) VALUES
                (1, 'Action'),
                (2, 'Comedy'),
                (3, 'Horror'),
                (4, 'Thriller'),
                (5, 'Drama');
            INSERT INTO RatingFact (FilmID, Rating, Genres) VALUES
                (1, 4.5, '["Action", "Comedy"]'),
                (2, 3.0, '["Horror", "Thriller"]'),
                (3, 4.0, '["Drama"]'),
                (4, 4.7, '["Action", "Drama"]'),
                (5, 2.5, '["Comedy", "Horror"]');
            SELECT rf.FilmID, rf.Rating, gd.GenreName
                FROM RatingFact rf
                JOIN GenreDim gd ON 
                json_extract(rf.Genres, '$') LIKE '%' || gd.GenreName || '%'
                LIMIT 10;
        """

        # Execute the query to fetch all table names
        result = conn.execute(query).fetchall()

        # Log the result
        logging.info("Tables in DuckDB schema:")
        for row in result:
            print("fSiin")
            print(row)
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
