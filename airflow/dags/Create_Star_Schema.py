from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime, timedelta

# Function to execute SQL queries in DuckDB
def execute_duckdb_query(query, database='/opt/airflow/star_schema.db'):
    try:
        conn = duckdb.connect(database=database, read_only=False)
        conn.execute(query)
        conn.close()
    except Exception as e:
        raise Exception(f"Error executing DuckDB query: {e}")

# Define the default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id='create_star_schema_duckdb',
        default_args=default_args,
        description='Create Star Schema Tables for the Movie Database using DuckDB',
        schedule_interval=None,  # Manual execution
        start_date=datetime(2024, 12, 1),
        catchup=False,
) as dag:

    # Define SQL queries
    create_user_dimension_sql = """
        CREATE TABLE IF NOT EXISTS User_dimension (
            id BIGINT PRIMARY KEY,
            gender VARCHAR,
            age INT,
            nationality VARCHAR
        );
    """

    create_genre_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Genre_dimension (
            id BIGINT PRIMARY KEY,
            genre1 VARCHAR,
            genre2 VARCHAR,
            genre3 VARCHAR,
            age_limit VARCHAR
        );
    """

    create_keyword_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Keyword_dimension (
            id BIGINT PRIMARY KEY,
            keyword1 VARCHAR,
            keyword2 VARCHAR,
            keyword3 VARCHAR
        );
    """

    create_movie_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Movie_dimension (
            id BIGINT PRIMARY KEY,
            title VARCHAR,
            original_lang VARCHAR,
            overview VARCHAR
        );
    """

    create_crew_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Crew_dimension (
            id BIGINT PRIMARY KEY,
            gender VARCHAR,
            name VARCHAR
        );
    """

    create_search_crew_bridge_sql = """
        CREATE TABLE IF NOT EXISTS Search_Crew_bridge (
            id BIGINT PRIMARY KEY,
            crewmate_ID BIGINT,
            job VARCHAR,
            department VARCHAR
        );
    """

    create_cast_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Cast_dimension (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            gender VARCHAR
        );
    """

    create_search_cast_bridge_sql = """
        CREATE TABLE IF NOT EXISTS Search_Cast_bridge (
            id BIGINT PRIMARY KEY,
            actor_ID BIGINT,
            character_name VARCHAR
        );
    """

    create_date_dimension_sql = """
        CREATE TABLE IF NOT EXISTS Date_dimension (
            id BIGINT PRIMARY KEY,
            release_date DATE,
            is_christmas BOOLEAN,
            is_new_year BOOLEAN,
            is_summer BOOLEAN,
            is_spring BOOLEAN,
            is_thanksgiving BOOLEAN,
            is_halloween BOOLEAN,
            is_valentines BOOLEAN
        );
    """

    create_search_fact_sql = """
        CREATE TABLE IF NOT EXISTS Search_fact (
            id BIGINT PRIMARY KEY,
            user_ID BIGINT,
            movie_ID BIGINT,
            genre_ID BIGINT,
            crew_ID BIGINT,
            keyword_ID BIGINT,
            cast_ID BIGINT,
            release_date_ID BIGINT,
            rating_value FLOAT,
            run_time INT,
            high_budget BOOLEAN,
            prod_company VARCHAR,
            prod_country VARCHAR
        );
    """

    # Define tasks
    create_user_dimension = PythonOperator(
        task_id='create_user_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_user_dimension_sql],
    )

    create_genre_dimension = PythonOperator(
        task_id='create_genre_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_genre_dimension_sql],
    )

    create_keyword_dimension = PythonOperator(
        task_id='create_keyword_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_keyword_dimension_sql],
    )

    create_movie_dimension = PythonOperator(
        task_id='create_movie_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_movie_dimension_sql],
    )

    create_crew_dimension = PythonOperator(
        task_id='create_crew_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_crew_dimension_sql],
    )

    create_search_crew_bridge = PythonOperator(
        task_id='create_search_crew_bridge',
        python_callable=execute_duckdb_query,
        op_args=[create_search_crew_bridge_sql],
    )

    create_cast_dimension = PythonOperator(
        task_id='create_cast_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_cast_dimension_sql],
    )

    create_search_cast_bridge = PythonOperator(
        task_id='create_search_cast_bridge',
        python_callable=execute_duckdb_query,
        op_args=[create_search_cast_bridge_sql],
    )

    create_date_dimension = PythonOperator(
        task_id='create_date_dimension',
        python_callable=execute_duckdb_query,
        op_args=[create_date_dimension_sql],
    )

    create_search_fact = PythonOperator(
        task_id='create_search_fact',
        python_callable=execute_duckdb_query,
        op_args=[create_search_fact_sql],
    )

    # Set task dependencies
    (
            create_user_dimension
            >> create_genre_dimension
            >> create_keyword_dimension
            >> create_movie_dimension
            >> create_crew_dimension
            >> create_search_crew_bridge
            >> create_cast_dimension
            >> create_search_cast_bridge
            >> create_date_dimension
            >> create_search_fact
    )
