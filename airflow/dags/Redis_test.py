from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3  # Update with your database connection module
import redis
import duckdb

# Step 1: Configure the Redis connection
redis_client = redis.StrictRedis(
    host='redis',  # Replace with your Redis server's hostname
    port=6379,         # Default Redis port
    decode_responses=True
)

# Step 2: Test the Redis connection
try:
    redis_client.ping()
    print("Connected to Redis!")
except redis.ConnectionError:
    print("Failed to connect to Redis.")
    raise

# Define functions for querying the SQLite database
def query_user_dimension():
    conn = sqlite3.connect('your_database.db')  # Update with your DB connection
    query = "SELECT * FROM User_dimension;"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    print("User_dimension data:", results)
    return results

def query_cast_dimension():
    conn = sqlite3.connect('your_database.db')  # Update with your DB connection
    query = "SELECT * FROM Cast_dimension;"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    print("Cast_dimension data:", results)
    return results

def query_search_cast_bridge():
    conn = sqlite3.connect('your_database.db')  # Update with your DB connection
    query = "SELECT * FROM Search_Cast_Bridge;"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    print("Search_Cast_Bridge data:", results)
    return results

# Define functions for loading data into DuckDB
def load_users_csv_to_user_dimension():
    conn = duckdb.connect('star_schema.db')
    query = "COPY User_dimension FROM 'path_to_users.csv' (HEADER);"
    conn.execute(query)
    conn.close()

def load_cast_csv_to_cast_dimension():
    conn = duckdb.connect('star_schema.db')
    query = "COPY Cast_dimension FROM 'path_to_cast.csv' (HEADER);"
    conn.execute(query)
    conn.close()

def load_data_to_search_cast_bridge():
    conn = duckdb.connect('star_schema.db')
    query = "COPY Search_Cast_Bridge FROM 'path_to_search_cast_bridge.csv' (HEADER);"
    conn.execute(query)
    conn.close()

# Default arguments
default_args = {
    'owner': 'pirjo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'movie_data_pipeline',
    default_args=default_args,
    description='A DAG for processing movie data',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 10),
    catchup=False,
)

# Define tasks for querying dimensions
task_query_user_dimension = PythonOperator(
    task_id='query_user_dimension',
    python_callable=query_user_dimension,
    dag=dag,
)

task_query_cast_dimension = PythonOperator(
    task_id='query_cast_dimension',
    python_callable=query_cast_dimension,
    dag=dag,
)

task_query_search_cast_bridge = PythonOperator(
    task_id='query_search_cast_bridge',
    python_callable=query_search_cast_bridge,
    dag=dag,
)

# Define tasks for loading data into DuckDB
task_load_users = PythonOperator(
    task_id='load_users_csv_to_user_dimension',
    python_callable=load_users_csv_to_user_dimension,
    dag=dag,
)

task_load_cast = PythonOperator(
    task_id='load_cast_csv_to_cast_dimension',
    python_callable=load_cast_csv_to_cast_dimension,
    dag=dag,
)

task_load_search_cast_bridge = PythonOperator(
    task_id='load_data_to_search_cast_bridge',
    python_callable=load_data_to_search_cast_bridge,
    dag=dag,
)

# Task dependencies
task_query_user_dimension >> task_query_cast_dimension >> task_query_search_cast_bridge
task_load_users >> task_load_cast >> task_load_search_cast_bridge
