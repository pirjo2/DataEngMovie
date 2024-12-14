import json
import pandas as pd
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def ingest_movielens(**kwargs):
    with open('../movie_data/raw_unstructured_data.json', 'r') as f:
        data = json.load(f)

    print("Movielens Data Loaded")
    return data

def ingest_tmdb(**kwargs):
    movies = pd.read_csv("../movie_data/tmdb_5000_movies.csv")
    credits = pd.read_csv("../movie_data/tmdb_5000_credits.csv")

    print("TMDB Data Loaded")
    return movies, credits

def ingest_users(**kwargs):
    df = pd.read_csv('../movie_data/users.csv')

    print("User Data Loaded")
    return df

def preprocess_movielens(movielens):

    movies_data = []
    ratings_data = []

    for movie in movielens:
        external_links = movie['external_links']
        imdb_id = external_links.split(", ")[0].replace("IMDB ID: ", "")
        tmdb_id = external_links.split(", ")[1].replace("TMDB ID: ", "")
        movie_id = len(movies_data) + 1

        movies_data.append({
            "movieId": movie_id,
            "imdbId": imdb_id,
            "tmdbId": tmdb_id
        })


        for data in movie['ratings']:
            if "Rated" in data:
                parts = data.split(" by user ")
                rating = float(parts[0].replace("Rated ", ""))
                user_parts = parts[1].split(" at ")
                user_id = user_parts[0]

                ratings_data.append({
                    "user_id": user_id,
                    "movieId": movie_id,
                    "rating": rating
                })

    movies = pd.DataFrame(movies_data)
    ratings = pd.DataFrame(ratings_data)

    ratings['user_id'] = pd.to_numeric(ratings['user_id'], errors='coerce', downcast='integer')

    movies['imdbId'] = pd.to_numeric(movies['imdbId'], errors='coerce', downcast='integer')
    movies['tmdbId'] = pd.to_numeric(movies['tmdbId'], errors='coerce', downcast='integer')

    ratings_merged = pd.merge(ratings, movies[['movieId', 'tmdbId']], on='movieId', how='left')
    ratings_merged = ratings_merged.drop(columns=["movieId"])
    
    return ratings_merged

def age_restrictions(movies):
    restrictions = {
      "R": ["Horror"], 
      "PG-13": ["Action", "Thriller", "Crime", "Science Fiction", "War", "Western"],
      "PG": ["Romance", "Drama", "Comedy"]
    }

    def get_restriction(genres):
    
        for r, g in restrictions.items():
          genre_intersection = list(set(g) & set(genres))
    
          if len(genre_intersection) > 0:
            return r
    
        return "G"
    
    movies["age_restriction"] = movies["genres"].apply(get_restriction)
    
    return movies

def extract_keywords(kv_json, key):
    kv = json.loads(kv_json)
    return [i[key] for i in kv]

def preprocess_movies(tmdb):

    tmdb['keywords'] = tmdb['keywords'].apply(extract_keywords, args=('name',))
    tmdb['genres'] = tmdb['genres'].apply(extract_keywords, args=('name',))
    
    tmdb["production_companies"] = tmdb["production_companies"].apply(extract_keywords, args=('name',))
    tmdb['production_countries'] = tmdb['production_countries'].apply(extract_keywords, args=('name',))

    tmdb['spoken_languages'] = tmdb['spoken_languages'].apply(extract_keywords, args=('name',))

    tmdb['release_date'] = pd.to_datetime(tmdb['release_date'], errors='coerce')
    
    tmdb['budget'] = pd.to_numeric(tmdb['budget'], errors='coerce', downcast='integer')
    tmdb['high_budget'] = tmdb['budget'] > 1000000
    
    tmdb = tmdb.drop(columns=['homepage', 'tagline', 'status', 'budget', 'original_title', 'popularity', 'revenue', 'spoken_languages', 'vote_average', 'vote_count']) # missing for many, cannot aggregate / unnecessary
    
    tmdb = age_restrictions(tmdb)
    tmdb = tmdb.rename(columns={"id": "tmdbId"})
    
    return tmdb

def preprocess_cast_crew(credits):

    cast_df = []
    crew_df = []

    for _, movie in credits.iterrows():
        cast = json.loads(movie["cast"])
        crew = json.loads(movie["crew"])
        movie_id = movie["movie_id"]

        for actor in cast:
            cast_df.append({
                "tmdbId": movie_id,
                "name": actor["name"],
                "character": actor["character"],
                "gender": "F" if actor["gender"] == 1 else "M"
            })

        added_jobs = ["Director"] # Only directors are included, otherwise loading crew data takes a very long time
        for member in crew:
            if member["job"] in added_jobs:
                crew_df.append({
                "tmdbId": movie_id,
                "name": member["name"],
                "job": member["job"],
                "department": member["department"],
                "gender": "F" if member["gender"] == 1 else "M"
                })

    cast_df = pd.DataFrame(cast_df)
    crew_df = pd.DataFrame(crew_df)

    return cast_df, crew_df


def date_table(movies, **kwargs):
    holiday_df_data = []

    for _, row in movies.iterrows():
        movie_id = row["tmdbId"]
        date = row["release_date"]

        holidays = {
            "is_christmas": {"start": datetime(date.year, 12, 1), "end": datetime(date.year, 12, 25)},
            "is_new_year": {"start": datetime(date.year, 12, 1), "end": datetime(date.year + 1, 1, 1)},
            "is_thanksgiving": {"start": datetime(date.year, 11, 1), "end": datetime(date.year, 11, 30)},
            "is_halloween": {"start": datetime(date.year, 10, 1), "end": datetime(date.year, 10, 31)},
            "is_valentines": {"start": datetime(date.year, 2, 1), "end": datetime(date.year, 2, 14)},
        }

        summer = {"start": datetime(date.year, 6, 1), "end": datetime(date.year, 8, 31)}
        spring = {"start": datetime(date.year, 3, 1), "end": datetime(date.year, 5, 31)}

        result = {}
        result["tmdbId"] = movie_id
        result["release_date"] = date

        for holiday, range_ in holidays.items():
            result[holiday] = range_["start"] <= date <= range_["end"]

        result["is_summer"] = summer["start"] <= date <= summer["end"]
        result["is_spring"] = spring["start"] <= date <= spring["end"]

        holiday_df_data.append(result)

    holiday_df = pd.DataFrame(holiday_df_data, columns=["tmdbId", "release_date"] + list(holidays.keys()) + ["is_summer", "is_spring"])

    return holiday_df


def process_data(ti, **kwargs):
    print("Data ingested, started processing.")

    # Ensure the directory exists
    output_dir = '../movie_data'
    os.makedirs(output_dir, exist_ok=True)

    movielens_raw = ti.xcom_pull(task_ids='ingest_movielens')
    tmdb, credits = ti.xcom_pull(task_ids='ingest_tmdb')

    ratings = preprocess_movielens(movielens_raw)

    movie_intersection = list(set(tmdb["id"].unique()) & set(ratings["tmdbId"].unique()))

    tmdb = tmdb[tmdb["id"].isin(movie_intersection)]
    credits = credits[credits["movie_id"].isin(movie_intersection)]

    # Preproccesed datasets
    ratings = ratings[ratings["tmdbId"].isin(movie_intersection)]

    tmdb = preprocess_movies(tmdb)
    cast_df, crew_df = preprocess_cast_crew(credits)

    holiday_df = date_table(tmdb)

    ratings.to_csv(f'{output_dir}/ratings.csv', index=False)
    tmdb.to_csv(f'{output_dir}/tmdb.csv', index=False)
    cast_df.to_csv(f'{output_dir}/cast.csv', index=False)
    crew_df.to_csv(f'{output_dir}/crew.csv', index=False)
    holiday_df.to_csv(f'{output_dir}/holidays.csv', index=False)

    print("Processed DataFrames saved as files.")

with DAG(
    dag_id='movie_data_processing_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Ingest Movielens data
    ingest_movielens_task = PythonOperator(
        task_id='ingest_movielens',
        python_callable=ingest_movielens,
    )

    # Ingest TMDB data
    ingest_tmdb_task = PythonOperator(
        task_id='ingest_tmdb',
        python_callable=ingest_tmdb,
    )

    # Ingest user data
    ingest_users_task = PythonOperator(
        task_id='ingest_users',
        python_callable=ingest_users,
    )

    # Preprocess all data
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    trigger_schema_creation = TriggerDagRunOperator(
        task_id='trigger_schema_creation',
        trigger_dag_id='create_star_schema_duckdb',
    )

    # Task dependencies
    [ingest_movielens_task, ingest_tmdb_task, ingest_users_task] >> process_data_task >> trigger_schema_creation
