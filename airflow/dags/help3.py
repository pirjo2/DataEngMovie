import streamlit as st
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime, timedelta

# Function to load data from DuckDB
def load_data():
    try:
        # Connect to DuckDB
        conn = duckdb.connect(database="star_schema.db")


        query = """
        SELECT
            md.title AS "Title",
            dd.relase_date AS "Release Year",
            AVG(sf.rating_value) AS "Rating",
            crd.name AS "Director",
            gd.genre1 AS "Genre",
            gd.age_limit AS "Age limit",
            kd.keyword1 AS "Tag"
        FROM Search_fact AS sf
        LEFT JOIN Movie_dimension AS md ON md.id = sf.movie_ID
        LEFT JOIN Date_dimension AS dd ON sf.relase_date_ID = dd.relase_date
        LEFT JOIN Search_Cast_bridge AS scb ON sf.cast_ID = scb.id
        LEFT JOIN Cast_dimension AS cd ON scb.actor_ID = cd.id
        LEFT JOIN Search_Crew_bridge AS scrb ON sf.crew_ID = scrb.id
        LEFT JOIN Crew_dimension AS crd ON scrb.crewmate_ID = crd.id
        LEFT JOIN Genre_dimension AS gd ON sf.genre_ID = gd.id
        LEFT JOIN Keyword_dimension AS kd ON sf.keyword_ID = kd.id
        WHERE sf.rating_value IS NOT NULL
        GROUP BY sf.movie_ID
        LIMIT 5;
    
        """
        data = conn.execute(query).fetchdf()

        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        raise e
    finally:
        if conn:
            print("Closing DuckDB connection...")
            conn.close()

# Function to filter and sort the movies based on provided filters
def filter_and_sort_movies2(data, filters_applied, sort_option):
    filters_applied = data
    # Apply filters to data
    if filters_applied.get('release_year'):
        data = data[(data["release_date"] >= filters_applied['release_year'][0]) &
                    (data["release_date"] <= filters_applied['release_year'][1])]

    if filters_applied.get('rating'):
        data = data[(data["rating_value"] >= filters_applied['rating'][0]) &
                    (data["rating_value"] <= filters_applied['rating'][1])]

    # Sorting
    if sort_option == "Rating (Highest-Lowest)":
        data = data.sort_values("rating_value", ascending=False)
    elif sort_option == "Rating (Lowest-Highest)":
        data = data.sort_values("rating_value", ascending=True)

    return data

# Streamlit function to display filtered data
def display_data(data):
    st.subheader("Movies")
    if data.empty:
        st.write("No movies found with the selected filters.")
    else:
        st.write(data)

# Airflow Task: Function to fetch and process the data
def airflow_task(filters_applied, sort_option):
    data = load_data()
    filtered_data = filter_and_sort_movies(data, filters_applied, sort_option)
    return filtered_data

# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 10),
    'retries': 1
}

dag = DAG(
    'OOOOOOOfilter_sort_movies_dag',
    default_args=default_args,
    description='A DAG to filter and sort movies',
    schedule_interval=None,
    catchup=False,
)

# Define task for filtering and sorting movies
filter_sort_task = PythonOperator(
    task_id='filter_and_sort_movies2',
    python_callable=airflow_task,
    dag=dag,
)
'''
# Streamlit App: Displaying filtered data
st.title('Trends in Cinema')

# Call the filtering function inside Streamlit
filters_applied = {
    'release_year': (2000, 2020),
    'rating': (0.0, 10.0),
}

sort_option = "Rating (Highest-Lowest)"
data = load_data()
filtered_data = filter_and_sort_movies2(data, filters_applied, sort_option)

# Display data with Streamlit
display_data(filtered_data)

'''
