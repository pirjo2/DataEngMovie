import streamlit as st
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import numpy as np
import duckdb
from datetime import datetime, timedelta
#from st_aggrid import AgGrid, GridOptionsBuilder, JsCode #(pip install streamlit-aggrid)

#st.image("camera.png")
#st.title('Trends in Cinema')
'''
# Function to fetch and print table names
def print_table_names():
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(database="star_schema.db")
        # Execute the query and fetch as a DataFrame
        query = "SELECT * FROM Search_fact"
        data = conn.execute(query).fetchdf()

        # Display the data
        print(data)
        # Fetch all table names
        result = conn.execute("SHOW SCHEMAS;").fetchall()

        # Print table names
        table_names = [row[0] for row in result]
        print("Tables in star_schema.db:")
        for table_name in table_names:
            print(f"- {table_name}")

        # Close the connection
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Error fetching table names: {e}")
'''
data = ''
def load_data():
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(database="star_schema.db")

    # Write the query to fetch the data
        '''query = """
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
        '''

        query = """
        SHOW TABLES;
        """


        # Execute the query and fetch the data as a DataFrame
        data = conn.execute(query).fetchdf()

        res = [row[0] for row in data]
        print("RESULT:")
        for r in res:
            print(f"- {r}")
        #data = conn.execute(query).fetchall()
    except Exception as e:
        print(f"Error querying first 5 rows: {e}")
        raise e
    finally:
        if conn:
            print("Closing DuckDB connection...")
            conn.close()

    return data

# Load the data
data = load_data()




def filter_and_sort_movies(**kwargs):
    # Load data
    #data = pd.read_csv(MOVIE_DATA_PATH)

    # Filters (replicating Streamlit options)
    filters_applied = kwargs['filters_applied']

    if filters_applied.get('release_year'):
        data = data[(data["Release Year"] >= filters_applied['release_year'][0]) &
                    (data["Release Year"] <= filters_applied['release_year'][1])]

    if filters_applied.get('rating'):
        data = data[(data["Rating"] >= filters_applied['rating'][0]) &
                    (data["Rating"] <= filters_applied['rating'][1])]

    if filters_applied.get('age_limit'):
        data = data[data["Age limit"].isin(filters_applied['age_limit'])]

    if filters_applied.get('category'):
        data = data[data["Category"].isin(filters_applied['category'])]

    if filters_applied.get('director'):
        data = data[data["Director"].isin(filters_applied['director'])]

    if filters_applied.get('genre'):
        data = data[data["Genre"].isin(filters_applied['genre'])]

    if filters_applied.get('tag'):
        data = data[data["Tag"].isin(filters_applied['tag'])]

    # Sorting
    sort_option = kwargs['sort_option']
    if sort_option == "Rating (Highest-Lowest)":
        data = data.sort_values("Rating", ascending=False)
    elif sort_option == "Rating (Lowest-Highest)":
        data = data.sort_values("Rating", ascending=True)
    elif sort_option == "Alphabetical (A-Z)":
        data = data.sort_values("Title", ascending=True)
    elif sort_option == "Alphabetical (Z-A)":
        data = data.sort_values("Title", ascending=False)
    elif sort_option == "Release Year (Newest-Oldest)":
        data = data.sort_values("Release Year", ascending=False)
    elif sort_option == "Release Year (Oldest-Newest)":
        data = data.sort_values("Release Year", ascending=True)

    # SHOWS REQUESTED MOVIES
    st.subheader("Movies")
    if filters_applied.empty:
        st.write("No movies found with the selected filters.")
    else:
        st.markdown(filters_applied.style.hide(axis="index").to_html(), unsafe_allow_html=True)

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 10),
    'retries': 1
}
# Define DAG
dag = DAG(
    'AAAAAAAAAAAAAAmovie_filter_sort_dag',
    default_args=default_args,
    description='A DAG to filter and sort movies',
    schedule_interval=None,  # Trigger manually or as needed
)

'''
# Define the PythonOperator task
fetch_table_names_task = PythonOperator(
    task_id='print_table_names',
    python_callable=print_table_names,
    dag=dag,
)'''

# Define task
filter_sort_task = PythonOperator(
    task_id='filter_and_sort_movies',
    python_callable=filter_and_sort_movies,
    op_kwargs={
        'filters_applied': {
            'release_year': (2000, 2020),
            'rating': (0.0, 10.0),
            'age_limit': ['PG-13', 'R'],
            'category': ['Action', 'Comedy'],
            'director': ['Steven Spielberg'],
            'genre': ['Adventure', 'Drama'],
            'tag': ['Oscar-winning']
        },
        'sort_option': "Rating (Highest-Lowest)"
    },
    dag=dag,
)

filter_sort_task
