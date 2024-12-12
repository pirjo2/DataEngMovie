'''from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from airflow.operators.python import PythonOperator

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Get the absolute path to `main.py`
#base_path = os.path.dirname(os.path.abspath(__file__))  # Get current script's directory
#print(f"base")
#print(f"{base_path}")
#streamlit_script_path = os.path.join(base_path, '../src/streamlit/main.py')  # Adjust relative path
streamlit_path = ''

def aa(**kwargs):
    base_path = os.path.dirname(os.path.abspath(__file__))
    streamlit_path = os.path.join(base_path, "..", "streamlit", "main.py")
    streamlit_path = os.path.abspath(streamlit_path)
    print("Resolved Path:", streamlit_path)
    # Push the resolved path to XCom for other tasks to use
    kwargs['ti'].xcom_push(key='streamlit_path', value=streamlit_path)



# Define the DAG
with DAG(
        'streamlit_app_execution',
        default_args=default_args,
        description='A DAG to run the Streamlit app',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    # Preprocess all data
    process_data_task = PythonOperator(
        task_id='aa',
        python_callable=aa,
        provide_context=True,
    )
    # Task to check Streamlit version
    check_streamlit_version = BashOperator(
        task_id='check_streamlit_version',
        bash_command='streamlit --version',
    )
    run_streamlit_app = BashOperator(
        task_id='run_streamlit',
        bash_command='streamlit run {{ ti.xcom_pull(task_ids="aa", key="streamlit_path") }}'
    )




from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def load_data():
    """Load initial movie data."""
    data = pd.DataFrame({
        "Title": ["B", "A", "D", "C"],
        "Release Year": [2000, 2010, 2020, 2005],
        "Rating": [5.0, 4.2, 3.1, 1.4],
        "Director": ["Director 1", "Director 2", "Director 1", "Director 3"],
        "Genre": ["Action", "Drama", "Action", "Comedy"],
        "Tag": ["Epic", "Sad", "Thrilling", "Funny"],
        "Age limit": ["G", "PG", "PG-13", "R"],
        "Category": ["Christmas", "Thanksgiving", "New Years", "Spring"],
        "Description": [
            "An epic tale of adventure and bravery.",
            "A heartwarming drama that explores the depths of human emotions.",
            "A thrilling action film with stunning visuals.",
            "A comedy that will leave you in stitches."
        ],
    })
    data.to_csv('/tmp/movies.csv', index=False)


def filter_data():
    """Apply filters to the movie data."""
    data = pd.read_csv('/tmp/movies.csv')
    # Example filter: Only include movies with a rating >= 4.0
    filtered_data = data[data['Rating'] >= 4.0]
    filtered_data.to_csv('/tmp/filtered_movies.csv', index=False)


def sort_data():
    """Sort filtered movie data."""
    filtered_data = pd.read_csv('/tmp/filtered_movies.csv')
    # Example sort: By Release Year, descending
    sorted_data = filtered_data.sort_values("Release Year", ascending=False)
    sorted_data.to_csv('/tmp/sorted_movies.csv', index=False)


def report_results():
    """Print the final sorted data."""
    sorted_data = pd.read_csv('/tmp/sorted_movies.csv')
    print("Final Movie Data:")
    print(sorted_data)

# Define the DAG
with DAG(
        'cinema_data_pipeline',
        default_args=default_args,
        description='A DAG to process and sort cinema data',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    # Task definitions
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    task_filter_data = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data
    )

    task_sort_data = PythonOperator(
        task_id='sort_data',
        python_callable=sort_data
    )

    task_report_results = PythonOperator(
        task_id='report_results',
        python_callable=report_results
    )

    # Task dependencies
    task_load_data >> task_filter_data >> task_sort_data >> task_report_results
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import pandas as pd

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}



# Define the DAG for cinema data processing from a database
def load_data_from_db():
    """Load data from star_schema.db directly into a pandas DataFrame."""
    conn = duckdb.connect(database="star_schema.db")
    try:
        query = """
        SELECT
            movie_ID, title, release_date, rating, director, genre, tag, age_limit, category, description
        FROM
            Movie_dimension;
        """
        data = conn.execute(query).fetchdf()
    finally:
        conn.close()
    return data

def filter_data_from_db():
    """Apply filters to the data loaded from the database."""
    data = load_data_from_db()
    # Example filter: Only include movies with a rating >= 4.0
    filtered_data = data[data['rating'] >= 4.0]
    return filtered_data

def sort_data_from_db():
    """Sort filtered movie data."""
    filtered_data = filter_data_from_db()
    # Example sort: By Release Year, descending
    sorted_data = filtered_data.sort_values("release_year", ascending=False)
    return sorted_data

def report_results_from_db():
    """Print the final sorted data."""
    sorted_data = sort_data_from_db()
    print("Final Movie Data:")
    print(sorted_data)

# Define the DAG for processing cinema data
with DAG(
        'cinema_data_pipeline',
        default_args=default_args,
        description='A DAG to process and sort cinema data from the DuckDB database',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    # Task definitions
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_from_db
    )

    task_filter_data = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data_from_db
    )

    task_sort_data = PythonOperator(
        task_id='sort_data',
        python_callable=sort_data_from_db
    )

    task_report_results = PythonOperator(
        task_id='report_results',
        python_callable=report_results_from_db
    )

    # Task dependencies
    task_load_data >> task_filter_data >> task_sort_data >> task_report_results