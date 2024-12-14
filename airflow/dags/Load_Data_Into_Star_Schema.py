from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import duckdb
from datetime import datetime, timedelta

CSV_FILE_PATHS = {
    'User_dimension': '../movie_data/users.csv',
    'Genre_dimension': '../movie_data/tmdb.csv',
    'Keyword_dimension': '../movie_data/tmdb.csv',
    'Date_dimension': '../movie_data/holidays.csv',
    'Movie_dimension': '../movie_data/tmdb.csv',
    'Crew_dimension': '../movie_data/crew.csv',
    'Cast_dimension': '../movie_data/cast.csv',
    'Search_Cast_bridge': '../movie_data/cast.csv',
    'Search_Crew_bridge': '../movie_data/crew.csv',
    'Search_fact': '../movie_data/ratings.csv',
}

# Function to fetch row count from a DuckDB table
def get_db_row_count(table_name):
    conn = duckdb.connect(database="star_schema.db", read_only=True)
    query = f"SELECT COUNT(*) FROM {table_name}"
    result = conn.execute(query).fetchone()
    conn.close()
    return result[0]  # Return the count of rows

# Function to get row count from a CSV file
def get_csv_row_count(csv_file_path, get_unique):
    df = pd.read_csv(csv_file_path)
    if get_unique:
        # Actor and crewmate dimensions should only include unique gender and name paris
        return df[['name', 'gender']].drop_duplicates().shape[0]
    return len(df)

# Python function to compare the row counts
def compare_row_counts(table_name, **kwargs):
    csv_file_path = CSV_FILE_PATHS[table_name]
    get_unique = False

    if ("Crew" in table_name or "Cast" in table_name) and "bridge" not in table_name:
        get_unique = True

    # Get row count from DB and CSV
    db_row_count = get_db_row_count(table_name)
    csv_row_count = get_csv_row_count(csv_file_path, get_unique)

    # Check if the row counts match
    if db_row_count == csv_row_count:
        print(f"Row count matches for {table_name}: {db_row_count} rows.")
    else:
        print(f"Row count mismatch for {table_name}: DB ({db_row_count} rows), CSV ({csv_row_count} rows).")
        raise ValueError(f"Row count mismatch for {table_name}.")

def preprocess_gk(series, colname):
    series = series.fillna('').str.replace(r'[\[\]]', '', regex=True)
    split = series.str.split(',', expand=True).iloc[:, :3]
    split = split.apply(lambda x: pd.concat([x, pd.Series([''] * (3 - len(x)))]).head(3), axis=1)
    split.columns = [f'{colname}1', f'{colname}2', f'{colname}3']
    return split


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
        users_df.to_sql('User_dimension', conn, if_exists='replace', index=False)
        print("USRERS ARE DONE")
        # Load data into Date_dimension from holidays

        holidays_df = holidays_df.rename(columns={"tmdbId": "id"})
        holidays_df.to_sql('Date_dimension', conn, if_exists='replace', index=False, chunksize=1000)
        print("HOLIDAYS ARE DONE")

        # Load data into Genre_dimension
        genres_df = preprocess_gk(tmdb_df['genres'], "genre")
        tmdb_df[['genre1', 'genre2', 'genre3']] = genres_df
        tmdb_df['age_limit'] = tmdb_df['age_restriction'].fillna('')

        genre_dimension_df = tmdb_df[['tmdbId', 'genre1', 'genre2', 'genre3', 'age_limit']]
        genre_dimension_df = genre_dimension_df.rename(columns={"tmdbId": "id"})

        conn.execute("""
            CREATE TABLE IF NOT EXISTS Genre_dimension (
                id INTEGER, 
                genre1 VARCHAR, 
                genre2 VARCHAR, 
                genre3 VARCHAR, 
                age_limit VARCHAR
            );
        """)

        genre_dimension_df.to_sql('Genre_dimension', conn, if_exists='append', index=False)
        print("GENRES ARE DONE")

        # Load data into Keyword_dimension
        keywords_df = preprocess_gk(tmdb_df['keywords'], "keyword")
        tmdb_df[['keyword1', 'keyword2', 'keyword3']] = keywords_df
        keyword_dimension_df = tmdb_df[['tmdbId', 'keyword1', 'keyword2', 'keyword3']]
        keyword_dimension_df = keyword_dimension_df.rename(columns={"tmdbId": "id"})

        conn.execute("""
            CREATE TABLE IF NOT EXISTS Keyword_dimension (
                id INTEGER, 
                keyword1 VARCHAR, 
                keyword2 VARCHAR, 
                keyword3 VARCHAR
            );
        """)

        keyword_dimension_df.to_sql('Keyword_dimension', conn, if_exists='append', index=False)
        print("KEYWORDS ARE DONE")

        def insert_dimension_data(df, dimension_name, id_column, name_column, gender_column):
            # Create a DataFrame with unique names and genders
            unique_df = df[[name_column, gender_column]].drop_duplicates().reset_index(drop=True)
            unique_df[id_column] = range(1, len(unique_df) + 1)  # Generate unique IDs

            # Insert the data into the dimension table
            dimension_data = unique_df[[id_column, gender_column, name_column]]

            # Create the table if it doesn't exist, now using BIGINT for primary key
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {dimension_name} (
                    {id_column} BIGINT PRIMARY KEY,  -- Use BIGINT for the primary key
                    gender VARCHAR,
                    name VARCHAR
                );
            """)

            # Insert into the dimension table in bulk using pandas to_sql
            dimension_data.to_sql(dimension_name, conn, if_exists='append', index=False)

            # Return a dictionary mapping names to IDs
            return unique_df.set_index(name_column)[id_column].to_dict()

        # Insert unique crew data into Crew_dimension and get name-to-ID mapping
        crew_name_to_id = insert_dimension_data(crew_df, 'Crew_dimension', 'id', 'name', 'gender')

        # Insert unique cast data into Cast_dimension and get name-to-ID mapping
        cast_name_to_id = insert_dimension_data(cast_df, 'Cast_dimension', 'id', 'name', 'gender')

        def insert_search_bridge(df, name_to_id, bridge_table, id_column, name_column, role_column, department_column=None):
            # Map names to IDs
            df[id_column] = df[name_column].map(name_to_id)

            # Filter out rows with missing IDs
            df = df.dropna(subset=[id_column])

            # Prepare data for insertion into the bridge table
            if department_column:
                bridge_data = df[[id_column, role_column, department_column]]
            else:
                bridge_data = df[[id_column, role_column]]

            # Generate a unique 'id' for each row in the bridge table
            bridge_data['id'] = range(1, len(bridge_data) + 1)

            # Re-order the columns to match the required schema
            if department_column:
                bridge_data = bridge_data[['id', id_column, role_column, department_column]]
                conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {bridge_table} (
                            id BIGINT PRIMARY KEY,  -- Bridge table's ID is a primary key (BIGINT)
                            {id_column} BIGINT,     -- Foreign key connecting to dimension tables
                            {role_column} VARCHAR,  -- Cast role/character name
                            department VARCHAR      -- Department of the crew/cast member
                        );
                    """)
            else:
                bridge_data = bridge_data[['id', id_column, role_column]]
                bridge_data = bridge_data.rename(columns={"character": "character_name"})
                conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {bridge_table} (
                            id BIGINT PRIMARY KEY,
                            {id_column} BIGINT,
                            {role_column} VARCHAR,
                        );
                    """)

            bridge_data.to_sql(bridge_table, conn, if_exists='append', index=False)

        # Insert data into Search_Crew_bridge table
        insert_search_bridge(crew_df, crew_name_to_id, 'Search_Crew_bridge', 'crewmate_ID', 'name', 'job', 'department')
        print("CREW ARE DONE")

        # Insert data into Search_Cast_bridge table
        insert_search_bridge(cast_df, cast_name_to_id, 'Search_Cast_bridge', 'actor_ID', 'name', 'character')
        print("CAST ARE DONE")

        # Load data into movie dimension
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Movie_dimension (
                id BIGINT PRIMARY KEY,
                title VARCHAR,
                original_lang VARCHAR,
                overview VARCHAR
            );
        """)

        movie_df = tmdb_df[["tmdbId", "original_language", "overview", "title"]]
        movie_df = movie_df.rename(columns={"tmdbId": "id", "original_language": "original_lang"})
        movie_df.to_sql('Movie_dimension', conn, if_exists='replace', index=False)
        print("MOVIES ARE DONE")

        search_fact_generate_id = 1

        def fetch_ids_for_tmdb_ids(tmdb_ids, table_name, column_name):
            placeholders = ', '.join('?' for _ in tmdb_ids)  # Prepare placeholders for the query
            query = f"SELECT {column_name}, id FROM {table_name} WHERE {column_name} IN ({placeholders})"
            result = conn.execute(query, tuple(tmdb_ids)).fetchall()
            return dict(result)  # Return a dictionary mapping the tmdb_id to its corresponding id

        # Helper function to get the tmdb_row from the dataframe
        def get_tmdb_rows(tmdb_ids):
            return tmdb_df[tmdb_df['tmdbId'].isin(tmdb_ids)]  # Filter rows based on tmdb_ids

        # Prepare data in bulk instead of processing row-by-row
        search_fact_data = []

        # Prepare a list of tmdb_ids from ratings_df
        tmdb_ids = ratings_df['tmdbId'].unique()

        # Fetch all IDs from the dimension tables for the tmdb_ids
        genre_ids = fetch_ids_for_tmdb_ids(tmdb_ids, 'Genre_dimension', 'id')
        keyword_ids = fetch_ids_for_tmdb_ids(tmdb_ids, 'Keyword_dimension', 'id')
        crew_ids = fetch_ids_for_tmdb_ids(tmdb_ids, 'Search_Crew_bridge', 'id')
        cast_ids = fetch_ids_for_tmdb_ids(tmdb_ids, 'Search_Cast_bridge', 'id')
        release_date_ids = fetch_ids_for_tmdb_ids(tmdb_ids, 'Date_dimension', 'id')

        # Get the tmdb rows to extract additional info like production companies and countries
        tmdb_rows = get_tmdb_rows(tmdb_ids)

        # Iterate over ratings_df and prepare the data for insertion in bulk
        for _, row in ratings_df.iterrows():
            try:
                tmdb_row = tmdb_rows[tmdb_rows['tmdbId'] == row['tmdbId']]
                if tmdb_row.empty:
                    print(f"Warning: No matching tmdbId for {row['tmdbId']}")
                    continue  # Skip if no matching tmdb row is found

                # Extract production company and country directly
                prod_company = tmdb_row['production_companies'].iloc[0][1:-1].split(",")[0] if tmdb_row['production_companies'].notnull().any() else ''
                prod_country = tmdb_row['production_countries'].iloc[0][1:-1].split(",")[0] if tmdb_row['production_countries'].notnull().any() else ''

                # Fetch the necessary IDs for this row from the pre-fetched dictionaries
                genre_id = genre_ids.get(row['tmdbId'])
                keyword_id = keyword_ids.get(row['tmdbId'])
                crew_id = crew_ids.get(row['tmdbId'])
                cast_id = cast_ids.get(row['tmdbId'])
                release_date_id = release_date_ids.get(row['tmdbId'])

                # Prepare the row for insertion into the Search_fact table
                search_fact_data.append((
                    search_fact_generate_id,
                    int(row['user_id']),
                    int(row['tmdbId']),
                    float(row['rating']),
                    genre_id,
                    crew_id,
                    keyword_id,
                    cast_id,
                    release_date_id,
                    int(tmdb_row['runtime'].iloc[0]) if pd.notnull(tmdb_row['runtime']).any() else None,
                    bool(tmdb_row['high_budget'].iloc[0]) if pd.notnull(tmdb_row['high_budget']).any() else None,
                    prod_company,
                    prod_country
                ))

                # Increment the ID counter for the next row
                search_fact_generate_id += 1

            except KeyError as e:
                print(f"KeyError: Missing key {e} for row {row['user_id']} - {row['tmdbId']}")
            except Exception as e:
                print(f"Error processing row {row['user_id']} - {row['tmdbId']}: {e}")

        # Bulk insert into the Search_fact table
        query = """
            INSERT INTO Search_fact (
                id, user_ID, movie_ID, rating_value, genre_ID, crew_ID, 
                keyword_ID, cast_ID, release_date_ID, run_time, 
                high_budget, prod_company, prod_country
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        conn.executemany(query, search_fact_data)

        print("FACT IS DONE")

    except Exception as e:
        print(f"Error loading data into DuckDB: {e}")
        raise e
    finally:
        if conn:
            conn.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 4),
}

# Define DAG
with DAG(
        dag_id='load_data_into_star_schema',
        default_args=default_args,
        description='DAG to load data from csv to db and validate based on row counts',
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
) as dag:

    # Task to start Streamlit
    load_data_task = PythonOperator(
        task_id='load_data_to_star_schema',
        python_callable=load_data_to_duckdb,
        dag=dag,
    )

    # Validation tasks
    validation_tasks = []
    for table_name, csv_path in CSV_FILE_PATHS.items():
        task = PythonOperator(
            task_id=f'check_{table_name}_row_count',
            python_callable=compare_row_counts,
            op_kwargs={'table_name': table_name},  # Pass the table_name as a keyword argument
            provide_context=True,
        )
        validation_tasks.append(task)

    # Start_streamlit runs only after all validation tasks succeed
    load_data_task >> validation_tasks
