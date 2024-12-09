from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import duckdb
from datetime import datetime, timedelta

# Function to load the data into DuckDB from CSV files
def load_data_to_duckdb():
    try:
        # Establish DuckDB connection
        conn = duckdb.connect(database="star_schema.db")

        # Load the CSV files into Pandas DataFrames
        users_df = pd.read_csv('../movie_data/users.csv')
        ratings_df = pd.read_csv('../movie_data/ratings.csv')
        tmdb_df = pd.read_csv('../movie_data/tmdb.csv')
        cast_df = pd.read_csv('../movie_data/cast.csv')
        crew_df = pd.read_csv('../movie_data/crew.csv')
        holidays_df = pd.read_csv('../movie_data/holidays.csv')

        # Load data into UserDimension
        for _, row in users_df.iterrows():
            conn.execute(f"""
                INSERT INTO UserDimension (UserID, Name, Gender, Age)
                VALUES ({row['user_id']}, '{row['name']}', '{row['gender']}', {row['age']});
            """)

        # Load data into DateDimension from holidays
        for _, row in holidays_df.iterrows():
            conn.execute(f"""
                INSERT INTO DateDimension (DateID, Date, HolidayName)
                VALUES ({row['holiday_id']}, '{row['holiday_date']}', '{row['holiday_name']}');
            """)

        # Load data into GenreDimension
        for _, row in tmdb_df.iterrows():
            for genre in row['genres'].split('|'):  # Assuming genres are pipe-separated
                conn.execute(f"""
                    INSERT INTO GenreDimension (GenreID, GenreName)
                    VALUES ({row['tmdbId']}, '{genre}');
                """)

        # Load data into KeywordDimension
        for _, row in tmdb_df.iterrows():
            for keyword in row['keywords'].split('|'):  # Assuming keywords are pipe-separated
                conn.execute(f"""
                    INSERT INTO KeywordDimension (KeywordID, KeywordName)
                    VALUES ({row['tmdbId']}, '{keyword}');
                """)

        # Load data into MovieDimension
        for _, row in tmdb_df.iterrows():
            conn.execute(f"""
                INSERT INTO MovieDimension (MovieID, Title, OriginalLanguage, Overview)
                VALUES ({row['tmdbId']}, '{row['title']}', '{row['original_language']}', '{row['overview']}');
            """)

        # Load data into CrewDimension
        for _, row in crew_df.iterrows():
            conn.execute(f"""
                INSERT INTO CrewDimension (CrewID, Gender, Name)
                VALUES ({row['crew_id']}, '{row['gender']}', '{row['name']}');
            """)

        # Load data into SearchCrewBridge
        for _, row in crew_df.iterrows():
            conn.execute(f"""
                INSERT INTO SearchCrewBridge (CrewmateID, Job, Department)
                VALUES ({row['crew_id']}, '{row['job']}', '{row['department']}');
            """)

        # Load data into CastDimension
        for _, row in cast_df.iterrows():
            conn.execute(f"""
                INSERT INTO CastDimension (CastID, Gender, Name)
                VALUES ({row['cast_id']}, '{row['gender']}', '{row['name']}');
            """)

        # Load data into SearchCastBridge
        for _, row in cast_df.iterrows():
            conn.execute(f"""
                INSERT INTO SearchCastBridge (ActorID, CharacterName)
                VALUES ({row['cast_id']}, '{row['character']}');
            """)

        # Load data into SearchFact from ratings.csv and tmdb.csv
        for _, row in ratings_df.iterrows():
            tmdb_row = tmdb_df[tmdb_df['tmdbId'] == row['tmdbId']].iloc[0]
            conn.execute(f"""
                INSERT INTO SearchFact (UserID, MovieID, RatingValue, RunTime, HighBudget, ProdCompany, ProdCountry, ReleaseDateID)
                VALUES ({row['user_id']}, {tmdb_row['tmdbId']}, {row['rating']}, {tmdb_row['runtime']}, {tmdb_row['high_budget']}, 
                        '{tmdb_row['production_companies']}', '{tmdb_row['production_countries']}', {row['holiday_id']});
            """)

        # Close the connection
        conn.close()

    except Exception as e:
        raise RuntimeError(f"Error loading data into DuckDB: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 4),
}

# Define the DAG
dag = DAG(
    'load_data_into_star_schema',
    default_args=default_args,
    description='A DAG to load data into star schema from CSV files',
    schedule_interval=None,  # No schedule, will run manually
    catchup=False,
)

# Define the task
load_data_task = PythonOperator(
    task_id='load_data_to_star_schema',
    python_callable=load_data_to_duckdb,
    dag=dag,
)

load_data_task
