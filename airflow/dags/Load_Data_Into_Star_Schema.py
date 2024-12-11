from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import json

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
            try:
                # Get the genres, using an empty string as the default if not available
                genres = row.get('genres', '')  # Default to '[]' if genres is missing

                # Split the string by commas to simulate JSON list behavior
                if genres:
                    genres = [g.strip() for g in genres.split(',')]  # Split and strip whitespace
                else:
                    genres = []  # Default to empty list if no genres are provided


                # Ensure there are exactly 3 genres, filling with empty strings if fewer
                if len(genres) < 3:
                    genres.extend([''] * (3 - len(genres)))  # Fill with empty strings if fewer than 3 genres

                # Extract the first three genres (or empty strings if there are fewer than 3)
                genre1, genre2, genre3 = genres[:3]

                # Get the age limit (if available)
                age_limit = row.get('age_limit', None)  # Use None for SQL NULL

                # Parameterized query to avoid SQL injection
                query = """
                    INSERT INTO Genre_dimension (id, genre1, genre2, genre3, age_limit)
                    VALUES (?, ?, ?, ?, ?);
                """
                conn.execute(query, (row['tmdbId'], genre1, genre2, genre3, age_limit))

            except Exception as e:
                print(f"Genre Error processing row {row['tmdbId']}: {e}")


        for _, row in tmdb_df.iterrows():
            try:
                # Parse keywords as a string
                keywords_str = row.get('keywords', '')  # Use '' as default if keywords is missing
                keywords = [keyword.strip() for keyword in keywords_str.split(',') if keyword.strip()]  # Split and strip whitespace

                if len(keywords) < 3:
                    keywords.extend([''] * (3 - len(keywords)))  # Fill with empty strings if fewer than 3 keywords

                # Prepare data for insertion
                keyword1, keyword2, keyword3 = keywords[:3]

                # Parameterized query to avoid SQL injection
                query = """
                    INSERT INTO Keyword_dimension (id, keyword1, keyword2, keyword3)
                    VALUES (?, ?, ?, ?);
                """
                conn.execute(query, (row['tmdbId'], keyword1, keyword2, keyword3))

            except Exception as e:
                print(f"Keywords Error processing row {row['tmdbId']}: {e}")


        # Load data into Crew_dimension
        unique_crew = crew_df[['name', 'gender']].drop_duplicates().reset_index(drop=True)
        unique_crew['crewmateID'] = range(1, len(unique_crew) + 1)

        # Insert unique crew into Crew_dimension
        for _, row in unique_crew.iterrows():
            try:
                conn.execute("""
                    INSERT INTO Crew_dimension (id, gender, name)
                    VALUES (?, ?, ?);
                """, (row['crewmateID'], row['gender'], row['name']))
            except Exception as e:
                print(f"Error inserting crew into Crew_dimension for {row['name']}: {e}")

        # Map names to crewmate_IDs for Search_Crew_Bridge
        name_to_id = unique_crew.set_index('name')['crewmateID'].to_dict()

        search_crew_bridge_id = 1
        # Insert into Search_Crew_Bridge
        for _, row in crew_df.iterrows():
            try:
                crewmate_id = name_to_id.get(row['name'])
                if crewmate_id is None:
                    print(f"Warning: Missing crewmate_id for {row['name']}")
                    continue  # Skip this row if crewmate_id is not found
                conn.execute("""
                    INSERT INTO Search_Crew_bridge (id, crewmate_ID, job, department)
                    VALUES (?, ?, ?, ?);
                """, (search_crew_bridge_id, crewmate_id, row['job'], row['department']))

                # Increment the ID for the next row
                search_crew_bridge_id += 1

            except Exception as e:
                print(f"Error inserting into Search_Crew_bridge for {row['name']} with job {row['job']}: {e}")


        # Load data into Cast_dimension
        unique_cast = cast_df[['name', 'gender']].drop_duplicates().reset_index(drop=True)
        unique_cast['CastID'] = range(1, len(unique_cast) + 1)

        # Insert unique cast into Cast_dimension
        for _, row in unique_cast.iterrows():
            try:
                conn.execute("""
                    INSERT INTO Cast_dimension (id, gender, name)
                    VALUES (?, ?, ?);
                """, (row['CastID'], row['gender'], row['name']))
            except Exception as e:
                print(f"Error inserting cast into Cast_dimension for {row['name']}: {e}")

        # Map names to CastIDs for Search_Cast_Bridge
        name_to_cast_id = unique_cast.set_index('name')['CastID'].to_dict()

        search_cast_bridge_id = 1
        # Insert into Search_Cast_Bridge
        for _, row in cast_df.iterrows():
            try:
                cast_id = name_to_cast_id.get(row['name'])
                if cast_id is None:
                    print(f"Warning: Missing cast_id for {row['name']}")
                    continue  # Skip this row if cast_id is not found
                conn.execute("""
                    INSERT INTO Search_Cast_bridge (id, actor_ID, character_name)
                    VALUES (?, ?, ?);
                """, (search_cast_bridge_id, cast_id, row['character']))
                # Increment the ID for the next row
                search_cast_bridge_id += 1
            except Exception as e:
                print(f"Error inserting into Search_Cast_bridge for {row['name']} with character {row['character']}: {e}")

        search_fact_generate_id = 1
        # Load data into Search_fact from ratings.csv and tmdb.csv
        for _, row in ratings_df.iterrows():
            try:
                # Find the matching row in tmdb_df
                tmdb_row = tmdb_df[tmdb_df['tmdbId'] == row['tmdbId']]
                if tmdb_row.empty:
                    print(f"Warning: No matching tmdbId for {row['tmdbId']}")
                    continue
                tmdb_row = tmdb_row.iloc[0]

                # Extract production company and country directly
                prod_company = tmdb_row['production_companies'].split(",")[0] if tmdb_row['production_companies'] else ''
                prod_country = tmdb_row['production_countries'].split(",")[0] if tmdb_row['production_countries'] else ''

                # Fetch IDs from dimension tables
                def fetch_id(table_name, column_name, value):
                    result = conn.execute(f"SELECT id FROM {table_name} WHERE {column_name} = ?", (str(value),)).fetchone()
                    return result[0] if result else None

                genre_id = fetch_id("Genre_dimension", "id", row['tmdbId'])
                keyword_id = fetch_id("Keyword_dimension", "id", row['tmdbId'])
                crew_id = fetch_id("Search_Crew_bridge", "id", row['tmdbId'])
                cast_id = fetch_id("Search_Cast_bridge", "id", row['tmdbId'])
                release_date_id = fetch_id("Date_dimension", "id", row['tmdbId'])

                # Convert numpy.int64 to standard Python types
                user_id = int(row['user_id'])
                movie_id = int(tmdb_row['tmdbId'])
                rating_value = float(row['rating'])  # Convert to float for safety
                run_time = int(tmdb_row['runtime']) if pd.notnull(tmdb_row['runtime']) else None
                high_budget = bool(tmdb_row['high_budget']) if pd.notnull(tmdb_row['high_budget']) else None

                # Insert data into Search_fact table
                query = """
                    INSERT INTO Search_fact (
                        id, user_ID, movie_ID, rating_value, genre_ID, crew_ID, 
                        keyword_ID, cast_ID, release_date_ID, run_time, 
                        high_budget, prod_company, prod_country
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                conn.execute(query, (
                    search_fact_generate_id,
                    user_id,
                    movie_id,
                    rating_value,
                    genre_id,
                    crew_id,
                    keyword_id,
                    cast_id,
                    release_date_id,
                    run_time,
                    high_budget,
                    prod_company,
                    prod_country
                ))
                search_fact_generate_id += 1
            except KeyError as e:
                print(f"KeyError: Missing key {e} for row {row['user_id']} - {row['tmdbId']}")
            except Exception as e:
                print(f"Error processing row {row['user_id']} - {row['tmdbId']}: {e}")

    except Exception as e:
        print(f"Error loading data into DuckDB: {e}, Failed on row: {row}")
        raise e
    finally:
        if conn:
            print("Closing DuckDB connection...")
            conn.close()

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
