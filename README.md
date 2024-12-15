# Data Engineering course project: Trends in Cinema
Authors: Anna Maria Tammin, Maria Anett Kaha, Pirjo Vainjärv

The aim of this project is to analyze and filter movies based on ratings and other attributes. This application helps users to filter out the highest-rated movies to discover and watch or filter out the best matches for users' desires. Also, if users want to watch the lowest-rated movies, then this application helps to filter them out.

Questions we were trying to answer were:
1. Are high budget movies on average rated higher than low budget movies?
2. What actor has participated in the most high-rated movies (movies with a rating of 4 or higher)?
3. Which director has on average the highest rated movies?
4. Which season has the highest rated movies?

(These questions are answered in the Streamlit application).
&nbsp;

### Source datasets:
[Movielens (Small)](https://grouplens.org/datasets/movielens/latest/) <br>
[TMDB 5000](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/data?select=tmdb_5000_movies.csv)

The Movielens dataset has been transformed into a semi-structured format for this project and additional randomly generated data was added.

### Star Schema
![drawSQL-image-export-2024-12-08](https://github.com/user-attachments/assets/e86916e8-46f6-437e-918a-25e94dafb686)
&nbsp;

## Running the Application

<b>Running this application requires Docker!</b>

The first step is to download the tmdb_5000_movies.csv and tmdb_5000_credits.csv from [TMDB 5000](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/data?select=tmdb_5000_movies.csv) into movie_data folder. These files are too large to be uploaded to git. Movielens data has already been added since it was modified for this project.

To run the application, navigate to <b>airflow</b> folder and run the following commands in order:
```
docker compose up airflow-init --remove-orphans
```
If previous finished with code 0:
```
docker compose up
```

The Apache Airflow is now running on localhost port 8081 and can be accessible from [http://localhost:8081/](http://localhost:8081/).

The <b>username and password</b> for Airflow are both "airflow".
&nbsp;

### Load Data into Database
Before triggering the pipeline (DAG <b>"movie_data_processing_pipeline"</b>), make sure all of the DAGs are unpaused (the toggle in front of DAG name is blue):

Trigger DAG <b>"movie_data_processing_pipeline"</b>.

After this DAG, the rest of the DAGs ("create_star_schema_duckdb" -> "load_data_into_star_schema") will be triggered automatically if the previous DAG has finished successfully. The whole pipeline takes about 15-20 minutes to finish (the dataset is very large).


### Starting Streamlit Application
Run the DAG <b>"start_streamlit"</b> to run the Streamlit application.
Streamlit is now running on localhost port 8051 and can be accessible from [http://localhost:8051/](http://localhost:8051/).

<b> The "start_streamlit" DAG will continue running until Streamlit application is closed by triggering the "stop_streamlit" DAG.</b> Both DAG-s should finish successfully.

### Closing the Application
1. Trigger DAG "stop_streamlit" to stop Streamlit
2. Use ctrl+C in terminal or stop the airflow container from Docker Desktop

Additionally:
```
docker compose down
```
&nbsp;

## Tools Used in the Project
<b>Airflow</b>: We use Airflow to orchestration. We have created DAG-s for all necessary tasks to initialize the database and start the Streamlit application. Some DAG-s also trigger other DAG-s, only 3 DAG-s have to be triggered manually.

<b>DuckDB</b>:We use DuckDB as our database.

<b>Streamlit</b>:We use streamlit as a visualization for our movie filtering and sorting system. Additionally, to display the answerd to the questions we wanted to answer with the project.

<b>Redis </b>: We use Redis to store frequent queries, the queries which answer our project questions. 
&nbsp;

## Changes in the Project Plan
As our project evoloved and we became more familiar with the tools used in the course, our project changed:
1. Instead of the IMDB dataset, we used the TMDB dataset because it contains more data about the movies (crew, cast, overview).
2. The questions we wanted to answer changed, we formulated more complex questions to utilize the all of our data.
3. We did not implement the synonym search using NLTK since the tags in the Movielens dataset were too unpredictable (some where barely words and other a whole passage) and we instead used the keywords in the TMBD dataset.
4. We do not use Neo4j to improve recommendation quality because we did not identify any complex relationships within the data.
