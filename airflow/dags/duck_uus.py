from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime, timedelta

# Function to query first five rows of all tables
def query_first_five_rows():
    try:
        # Establish DuckDB connection
        conn = duckdb.connect(database="star_schema.db")

        # List of tables to query
        tables = [
            "User_dimension", "Date_dimension", "Genre_dimension",
            "Keyword_dimension", "Crew_dimension", "Cast_dimension",
            "Search_Crew_bridge", "Search_Cast_bridge", "Search_fact"
        ]

        for table in tables:
            print(f"First 5 rows from {table}:")
            result = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
            print(result)

    except Exception as e:
        print(f"Error querying first 5 rows: {e}")
        raise e
    finally:
        if conn:
            print("Closing DuckDB connection...")
            conn.close()

# Function to validate foreign keys
def validate_foreign_keys():
    try:
        # Establish DuckDB connection
        conn = duckdb.connect(database="star_schema.db")

        # Define foreign key validations
        validations = {
            "Search_fact": {
                "user_ID": "User_dimension.id",
                "genre_ID": "Genre_dimension.id",
                "crew_ID": "Search_Crew_bridge.id",
                "keyword_ID": "Keyword_dimension.id",
                "cast_ID": "Search_Cast_bridge.id",
                "release_date_ID": "Date_dimension.id"
            }
        }

        for table, fks in validations.items():
            for fk, ref in fks.items():
                ref_table, ref_col = ref.split(".")
                query = f"""
                    SELECT COUNT(*) AS invalid_fk_count
                    FROM {table}
                    WHERE {fk} NOT IN (SELECT {ref_col} FROM {ref_table})
                """
                result = conn.execute(query).fetchone()
                if result[0] > 0:
                    print(f"Foreign key violation in {table}.{fk}: {result[0]} invalid references")
                else:
                    print(f"Foreign key {table}.{fk} is valid")

    except Exception as e:
        print(f"Error validating foreign keys: {e}")
        raise e
    finally:
        if conn:
            print("Closing DuckDB connection...")
            conn.close()

def query_and_print_data():
    try:
        # Establish DuckDB connection
        conn = duckdb.connect(database="star_schema.db")

        # Query: Join Search_fact with Cast and Crew through their respective bridges
        cast_crew_query = """
        SELECT 
            sf.id AS search_fact_id,
            sf.movie_ID, 
            cd.name AS cast_name, 
            scb.character_name,
            crd.name AS crew_name,
            scrb.job AS crew_job,
            scrb.department AS crew_department
        FROM Search_fact AS sf
        LEFT JOIN Search_Cast_bridge AS scb ON sf.cast_ID = scb.id
        LEFT JOIN Cast_dimension AS cd ON scb.actor_ID = cd.id
        LEFT JOIN Search_Crew_bridge AS scrb ON sf.crew_ID = scrb.id
        LEFT JOIN Crew_dimension AS crd ON scrb.crewmate_ID = crd.id
        LIMIT 5;
        """
        cast_crew_result = conn.execute(cast_crew_query).fetchall()
        print("Cast and Crew Join Results:")
        for row in cast_crew_result:
            print(row)

        # Query 2: Movies with Cast and Crew
        movie_cast_crew_query = """
        SELECT 
            sf.movie_ID, 
            cd.name AS cast_name, 
            crd.name AS crew_name
        FROM Search_fact AS sf
        LEFT JOIN Search_Cast_bridge AS scb ON sf.cast_ID = scb.id
        LEFT JOIN Cast_dimension AS cd ON scb.actor_ID = cd.id
        LEFT JOIN Search_Crew_bridge AS scrb ON sf.crew_ID = scrb.id
        LEFT JOIN Crew_dimension AS crd ON scrb.crewmate_ID = crd.id
        LIMIT 5;
        """
        movie_cast_crew_result = conn.execute(movie_cast_crew_query).fetchall()
        print("\nMovies with Cast and Crew:")
        for row in movie_cast_crew_result:
            print(row)

        # Query 3: Test Foreign Key Relationships
        fk_test_query = """
        SELECT
            sf.id AS search_fact_id,
            sf.user_ID AS user_id,
            sf.movie_ID AS movie_id,
            cd.name AS cast_name,
            crd.name AS crew_name,
            sf.prod_company AS production_company,
            sf.prod_country AS production_country
        FROM Search_fact AS sf
        LEFT JOIN Search_Cast_bridge AS scb ON sf.cast_ID = scb.id
        LEFT JOIN Cast_dimension AS cd ON scb.actor_ID = cd.id
        LEFT JOIN Search_Crew_bridge AS scrb ON sf.crew_ID = scrb.id
        LEFT JOIN Crew_dimension AS crd ON scrb.crewmate_ID = crd.id
        WHERE sf.cast_ID IS NOT NULL AND sf.crew_ID IS NOT NULL
        LIMIT 5;
        """
        fk_test_result = conn.execute(fk_test_query).fetchall()
        print("\nForeign Key Test Results:")
        for row in fk_test_result:
            print(row)

    except Exception as e:
        print(f"Error querying DuckDB: {e}")
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

# Define the DAG
dag = DAG(
    'validate_star_schema',
    default_args=default_args,
    description='A DAG to validate star schema data and foreign key relationships',
    schedule_interval=None,
    catchup=False,
)

# Define the tasks
query_task = PythonOperator(
    task_id='query_first_five_rows',
    python_callable=query_first_five_rows,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_foreign_keys',
    python_callable=validate_foreign_keys,
    dag=dag,
)

# Define the task
query_data_task = PythonOperator(
    task_id='query_cast_crew_data',
    python_callable=query_and_print_data,
    dag=dag,
)

# Set task dependencies
query_task >> validate_task >> query_data_task
