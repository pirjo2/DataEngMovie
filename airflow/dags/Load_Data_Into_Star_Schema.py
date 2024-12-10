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

        # Load data into User_dimension
        for _, row in users_df.iterrows():
            conn.execute("""
                INSERT INTO User_dimension (id, gender, age, nationality)
                VALUES (?, ?, ?, CAST(? AS VARCHAR));
            """, (row['user_id'], row['gender'], row['age'], row['nationality']))


        # Load data into Date_dimension from holidays
        for _, row in holidays_df.iterrows():
            conn.execute("""
                INSERT INTO Date_dimension (id, release_date, is_christmas, is_new_year, is_summer, is_spring, is_thanksgiving, is_halloween, is_valentines)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (row['tmdbId'], row['release_date'], row['is_christmas'], row['is_new_year'],
                  row['is_summer'], row['is_spring'], row['is_thanksgiving'], row['is_halloween'], row['is_valentines']))


        # Load data into Genre_dimension
        for _, row in tmdb_df.iterrows():
            genres = row.get('genres', [])
            if len(genres) < 3:
                genres.extend([''] * (3 - len(genres)))  # Fill with empty strings if fewer than 3 genres
            conn.execute(f"""
                    INSERT INTO Genre_dimension (id, genre1, genre2, genre3, age_limit)
                    VALUES ({row['tmdbId']}, '{genres[0]}', '{genres[1]}', '{genres[2]}', '{row.get('age_limit', 'NULL')}');
                """)

        # Load data into Keyword_dimension
        for _, row in tmdb_df.iterrows():
            keywords = row.get('keywords', [])
            if len(keywords) < 3:
                keywords.extend([''] * (3 - len(keywords)))  # Fill with empty strings if fewer than 3 genres
                conn.execute(f"""
                    INSERT INTO Keyword_dimension (id, keyword1, keyword2, keyword3)
                    VALUES ({row['tmdbId']}, '{keywords[0]}', '{keywords[1]}', '{keywords[2]}');
                """)

        # Load data into Movie_dimension
        for _, row in tmdb_df.iterrows():
            conn.execute(f"""
                INSERT INTO Movie_dimension (id, title, original_lang, overview)
                VALUES ({row['tmdbId']}, '{row['title']}', '{row['original_language']}', '{row['overview']}');
            """)

        # Step 1: Extract unique crew members
        unique_crew = crew_df[['name', 'gender']].drop_duplicates().reset_index(drop=True)

        # Add a unique CrewID to each unique crew member
        unique_crew['crewmateID'] = range(1, len(unique_crew) + 1)

        # Step 2: Insert into CrewDimension
        for _, row in unique_crew.iterrows():
            conn.execute(f"""
                INSERT INTO Crew_dimension (id, gender, name)
                VALUES ({row['crewmateID']}, '{row['gender']}', '{row['name']}');
            """)

        # Step 3: Map names to CrewIDs
        # Create a dictionary to map names to CrewIDs
        name_to_id = unique_crew.set_index('name')['CrewmateID'].to_dict()

        # Insert data into SearchCrewBridge
        for _, row in crew_df.iterrows():
            crewmate_id = name_to_id[row['name']]  # Get the corresponding CrewID
            conn.execute(f"""
                INSERT INTO Search_Crew_Bridge (crewmate_ID, job, department)
                VALUES ({crewmate_id}, '{row['job']}', '{row['department']}');
            """)

        # Step 1: Extract unique cast members
        unique_cast = cast_df[['name', 'gender']].drop_duplicates().reset_index(drop=True)

        # Add a unique CastID to each unique cast member
        unique_cast['CastID'] = range(1, len(unique_cast) + 1)

        # Step 2: Insert into Cast_dimension
        for _, row in unique_cast.iterrows():
            conn.execute(f"""
                INSERT INTO Cast_dimension (id, gender, name)
                VALUES ({row['CastID']}, '{row['gender']}', '{row['name']}');
            """)

        # Step 3: Map names to CastIDs
        # Create a dictionary to map names to CastIDs
        name_to_cast_id = unique_cast.set_index('name')['CastID'].to_dict()

        # Insert data into Search_Cast_Bridge
        for _, row in cast_df.iterrows():
            cast_id = name_to_cast_id[row['name']]  # Get the corresponding CastID
            conn.execute(f"""
                INSERT INTO Search_Cast_Bridge (id, actor_ID, character_name)
                VALUES ({row['tmdbId']}, {cast_id}, '{row['character']}');
            """)


    # Load data into Search_fact from ratings.csv and tmdb.csv
        for _, row in ratings_df.iterrows():
            tmdb_row = tmdb_df[tmdb_df['tmdbId'] == row['tmdbId']].iloc[0]
            conn.execute(f"""
                INSERT INTO Search_fact (user_ID, movie_ID, rating_value, run_time, high_budget, prod_company, prod_country, release_date_ID)
                VALUES ({row['user_id']}, {tmdb_row['tmdbId']}, {row['rating']}, {tmdb_row['runtime']}, {tmdb_row['high_budget']}, 
                        '{tmdb_row['production_companies']}', '{tmdb_row['production_countries']}', {row['holiday_id']});
            """)

        # Load data into Search_fact
        for _, row in ratings_df.iterrows():
            try:
                # Retrieve IDs for the current row
                tmdb_row = tmdb_df[tmdb_df['tmdbId'] == row['tmdbId']].iloc[0]

                # Retrieve GenreID (ensure genres are correctly inserted in GenreDimension)
                genre_id = conn.execute(f"""
                    SELECT id FROM Genre_dimension WHERE id = {row['tmdbId']}
                """).fetchone()[0]

                # Retrieve KeywordID
                keyword_id = conn.execute(f"""
                    SELECT id FROM Keyword_dimension WHERE id = {row['tmdbId']}
                """).fetchone()[0]

                # Retrieve CrewID (optional: fetch based on specific roles if needed)
                crew_id = conn.execute(f"""
                    SELECT id FROM Search_Crew_Bridge WHERE id = {row['tmdbId']}
                """).fetchone()[0]

                # Retrieve CastID
                cast_id = conn.execute(f"""
                    SELECT id FROM Search_Cast_Bridge WHERE id = {row['tmdbId']}
                """).fetchone()[0]

                # Retrieve ReleaseDateID
                release_date_id = conn.execute(f"""
                    SELECT id FROM Date_dimension WHERE id = {row['tmdbId']}
                """).fetchone()[0]

                # Insert into Search_fact
                conn.execute(f"""
                    INSERT INTO Search_fact (user_ID, movie_ID, rating_value, genre_ID, crew_ID, keyword_ID, cast_ID, release_date_ID, run_time, high_budget, prod_company, prod_country)
                    VALUES ({row['user_id']}, {tmdb_row['tmdbId']}, {row['rating']}, {genre_id}, {crew_id}, {keyword_id}, {cast_id}, {release_date_id}, 
                            {tmdb_row['runtime']}, {tmdb_row['high_budget']}, '{tmdb_row['production_companies']}', '{tmdb_row['production_countries']}');
                """)
            except Exception as e:
                print(f"Error processing row: {row}. Exception: {e}")


        # Close the connection
        conn.close()

    except Exception as e:
        raise RuntimeError(f"Error loading data into DuckDB: {e}, Failed on row: {row}")
        raise e

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
