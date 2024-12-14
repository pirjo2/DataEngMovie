# Data Engineering course project: Trends in Cinema
Authors: Anna Maria Tammin, Maria Anett Kaha, Pirjo Vainjärv

The aim of this project is to analyze and filter movies based on ratings and other attributes.

### Source datasets:
[Movielens (Small)](https://grouplens.org/datasets/movielens/latest/) <br>
[TMDB 5000](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/data?select=tmdb_5000_movies.csv)

The Movielens dataset has been transformed into a semi-structured format for this project and additional randomly generated data was added.

### Star Schema
![drawSQL-image-export-2024-12-08](https://github.com/user-attachments/assets/e86916e8-46f6-437e-918a-25e94dafb686)

## Running the Application

<b>Running this application requires Docker!</b>

The first step is to download the tmdb_5000_movies.csv and tmdb_5000_credits.csv from [TMDB 5000](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/data?select=tmdb_5000_movies.csv) into movie_data folder. These files are too large to be uploaded to git. Movielens data has already been added since it was modified for this project.

To run the application, run the following commands in order
```
docker compose airflow-init
```
If previous finished with code 0:
```
docker compose up
```

The Apache Airflow is now running on localhost port 8081 and can be accessible from [http://localhost:8081/](http://localhost:8081/).

The <b>username and password</b> for Airflow are both "airflow".


## Load Data into Database
First, make sure all of the following DAGs are unpaused:
1. create_star_schema_duckdb
2. load_data_into_star_schema
3. movie_data_processing_pipeline
4. start_streamlit
5. stop_streamlit

Trigger DAG <b>"movie_data_processing_pipeline"</b>.

After this DAG, the rest of the DAGs ("create_star_schema_duckdb" -> "load_data_into_star_schema") will be triggered automatically if the previous DAG has finished successfully.


### Starting Streamlit Application
Run the DAG <b>"start_streamlit"</b> to run the Streamlit application.

<b> The "start_streamlit" DAG will continue running until Streamlit application is closed by triggering the "stop_streamlit" DAG.</b> Both DAG-s should finish successfully.
