from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb

# Function to execute SQL queries
def execute_query(query):
    try:
        conn = duckdb.connect(database="star_schema.db")
        conn.execute(query)
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Error executing query: {e}")

# Define functions to add references to existing tables
def add_user_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (UserID) REFERENCES UserDimension(UserID);
    """
    execute_query(query)

def add_genre_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (GenreID) REFERENCES GenreDimension(GenreID);
    """
    execute_query(query)

def add_keyword_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (KeywordID) REFERENCES KeywordDimension(KeywordID);
    """
    execute_query(query)

def add_movie_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (MovieID) REFERENCES MovieDimension(MovieID);
    """
    execute_query(query)

def add_crew_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (CrewID) REFERENCES CrewDimension(CrewID);
    """
    execute_query(query)

def add_cast_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (CastID) REFERENCES CastDimension(CastID);
    """
    execute_query(query)

def add_release_date_reference():
    query = """
    ALTER TABLE SearchFact
    ADD FOREIGN KEY (ReleaseDateID) REFERENCES DateDimension(DateID);
    """
    execute_query(query)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 4),
}

# Define the DAG
dag = DAG(
    'alter_star_schema_references',
    default_args=default_args,
    description='A DAG to alter tables and add foreign key references in a star schema',
    schedule_interval=None,  # No schedule, will run manually
    catchup=False,
)

# Define the tasks
add_user_reference_task = PythonOperator(
    task_id='add_user_reference',
    python_callable=add_user_reference,
    dag=dag,
)

add_genre_reference_task = PythonOperator(
    task_id='add_genre_reference',
    python_callable=add_genre_reference,
    dag=dag,
)

add_keyword_reference_task = PythonOperator(
    task_id='add_keyword_reference',
    python_callable=add_keyword_reference,
    dag=dag,
)

add_movie_reference_task = PythonOperator(
    task_id='add_movie_reference',
    python_callable=add_movie_reference,
    dag=dag,
)

add_crew_reference_task = PythonOperator(
    task_id='add_crew_reference',
    python_callable=add_crew_reference,
    dag=dag,
)

add_cast_reference_task = PythonOperator(
    task_id='add_cast_reference',
    python_callable=add_cast_reference,
    dag=dag,
)

add_release_date_reference_task = PythonOperator(
    task_id='add_release_date_reference',
    python_callable=add_release_date_reference,
    dag=dag,
)

# Set task dependencies
(
        add_user_reference_task
        >> add_genre_reference_task
        >> add_keyword_reference_task
        >> add_movie_reference_task
        >> add_crew_reference_task
        >> add_cast_reference_task
        >> add_release_date_reference_task
)
