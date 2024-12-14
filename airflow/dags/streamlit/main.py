import streamlit as st
import duckdb
import pandas as pd

st.image("camera.png")
st.title('Trends in Cinema')

#@st.cache_data
def load_data():
    conn = duckdb.connect(database="/opt/airflow/star_schema.db", read_only=True)
    query = """
        SELECT 
        m.id AS movie_id,
        m.title,
        m.original_lang,
        m.overview,
        d.release_date,
        d.is_christmas, d.is_new_year, d.is_summer, d.is_spring, d.is_thanksgiving, d.is_halloween, d.is_valentines,
        g.age_limit, g.genre1, g.genre2, g.genre3,
        k.keyword1, k.keyword2, k.keyword3,
        c.name AS director_name,
        sf.rating_value,
        sf.run_time,
        sf.high_budget,
        sf.prod_company,
        sf.prod_country
    FROM Search_fact sf
    LEFT JOIN Movie_dimension m ON sf.movie_ID = m.id
    LEFT JOIN Date_dimension d ON sf.release_date_ID = d.id
    LEFT JOIN Genre_dimension g ON sf.genre_ID = g.id
    LEFT JOIN Keyword_dimension k ON sf.keyword_ID = k.id
    LEFT JOIN Crew_dimension c ON sf.crew_ID = c.id
    LEFT JOIN Search_Crew_bridge scb ON sf.crew_ID = scb.crewmate_ID AND scb.job = 'Director'
    LIMIT 10;
    """
    #LEFT JOIN Search_Cast_bridge scb2 ON sf.cast_ID = scb2.actor_ID
    #LEFT JOIN Cast_dimension cd ON scb2.actor_ID = cd.id
    result = conn.execute(query).fetch_df()

    result['release_date'] = pd.to_datetime(result['release_date'], errors='coerce')  # Convert to datetime

    return result

data = load_data()

@st.cache_data
def execute_query(query: str):
    conn = duckdb.connect(database="/opt/airflow/star_schema.db", read_only=True)
    return conn.execute(query).fetchall()

if "show_questions" not in st.session_state:
    st.session_state.show_questions = False

if  st.button("Show/Hide Questions"):
    st.session_state.show_questions = not st.session_state.show_questions

if st.session_state.show_questions:
    st.write("1. Are high budget movies on average rated higher than low budget movies?")
    query_1 = """
    SELECT 
    AVG(CASE WHEN high_budget THEN rating_value ELSE NULL END) > 
    AVG(CASE WHEN NOT high_budget THEN rating_value ELSE NULL END) AS high_budget_higher_rating
    FROM 
        Search_fact
    WHERE 
        rating_value IS NOT NULL;
    """
    result = execute_query(query_1)[0][0]
    st.markdown("**Query Result:**")
    st.write(str(result))

    st.write("2: What actor has participated in the most high-rated movies (movies with a rating of 4 or higher)?")
    query_2 = """
    SELECT 
    CD.name AS cast_member_name,
    COUNT(DISTINCT SF.movie_ID) AS high_rated_movie_count
    FROM 
        Search_fact SF
    JOIN 
        Search_Cast_bridge SCB ON SF.cast_ID = SCB.id
    JOIN 
        Cast_dimension CD ON SCB.actor_ID = CD.id
    WHERE 
        SF.rating_value >= 4
    GROUP BY 
        CD.name
    ORDER BY 
        high_rated_movie_count DESC
    LIMIT 100;

    """
    result = execute_query(query_2)
    st.markdown("**Query Result:**")
    st.write(result)

    st.write("3: Which director has on average the highest rated movies?")
    query_3 = """
    SELECT 
        MD.title AS movie_title,
        AVG(SF.rating_value) AS avg_rating
    FROM 
        Search_fact SF
    JOIN 
        Movie_dimension MD ON SF.movie_ID = MD.id
    WHERE 
        SF.rating_value IS NOT NULL
    GROUP BY 
        SF.movie_ID
    ORDER BY 
        avg_rating DESC
    LIMIT 4;
    """
    result = execute_query(query_3)
    st.markdown("**Query Result:**")
    st.write(result)

    st.write("4: Which season has the highest rated movies?")
    query_4 = """
    SELECT 
    CASE 
        WHEN DD.is_christmas THEN 'Christmas'
        WHEN DD.is_new_year THEN 'New Year'
        WHEN DD.is_summer THEN 'Summer'
        WHEN DD.is_spring THEN 'Spring'
        WHEN DD.is_thanksgiving THEN 'Thanksgiving'
        WHEN DD.is_halloween THEN 'Halloween'
        WHEN DD.is_valentines THEN 'Valentines'
        ELSE 'Other' 
    END AS season,
    AVG(SF.rating_value) AS avg_rating
    FROM 
        Search_fact SF
    JOIN 
        Date_dimension DD ON SF.release_date_ID = DD.id
    WHERE 
        SF.rating_value IS NOT NULL
    GROUP BY 
        season
    ORDER BY 
        avg_rating DESC
    LIMIT 1;
    """
    result = execute_query(query_4)
    st.markdown("**Query Result:**")
    st.write(result)

if "show_filters" not in st.session_state:
    st.session_state.show_filters = False

# FILTERS BUTTON
if st.button("Show/Hide Filters"):
    st.session_state.show_filters = not st.session_state.show_filters

# FILTERS SELECTION
filters_applied = False
if st.session_state.show_filters:
    st.subheader("Filters")

    apply_release_year = st.checkbox("Filter by release year", value=True)
    apply_rating = st.checkbox("Filter by rating", value=True)
    apply_age_limit = st.checkbox("Filter by age limit")
    apply_director = st.checkbox("Filter by director")
    apply_genre = st.checkbox("Filter by genre")
    apply_keyword = st.checkbox("Filter by keyword")

    if apply_release_year:
        filters_applied = True
        release_year_range = st.slider(
            "Release Year",
            int(data["release_date"].min().year),
            int(data["release_date"].max().year),
            (int(data["release_date"].min().year), int(data["release_date"].max().year))
        )

    if apply_rating:
        filters_applied = True
        rating_filter = st.slider("Rating", 0.0, 5.0, (0.0, 5.0))

    if apply_age_limit:
        filters_applied = True
        age_limit = st.multiselect("Age limit", data["age_limit"].unique())
    else:
        age_limit = []

    if apply_director:
        filters_applied = True
        directors = st.multiselect("Director",  data["director_name"].unique())
    else:
        directors = []

    if apply_genre:
        filters_applied = True
        genres = st.multiselect("Genre", data[["genre1", "genre2", "genre3"]].apply(lambda row: row.dropna().tolist(), axis=1).explode().unique())
    else:
        genres = []

    if apply_keyword:
        filters_applied = True
        keywords = st.multiselect("Tag", data[["keyword1", "keyword2", "keyword3"]].apply(lambda row: row.dropna().tolist(), axis=1).explode().unique())
    else:
        keywords = []

# MOVIE SORTING
sort_option = st.selectbox("Sort", ["Select...", "Rating (Highest-Lowest)",
                                    "Rating (Lowest-Highest)", "Alphabetical (A-Z)",
                                    "Alphabetical (Z-A)", "Release Year (Newest-Oldest)",
                                    "Release Year (Oldest-Newest)"])

filtered_data = data.copy()

# FILTERS
if st.session_state.show_filters:
    if apply_release_year:
        filtered_data = filtered_data[
            (filtered_data["release_date"].dt.year >= release_year_range[0]) &
            (filtered_data["release_date"].dt.year <= release_year_range[1])
            ]

    if apply_rating:
        filtered_data = filtered_data[
            (filtered_data["rating_value"] >= rating_filter[0]) &
            (filtered_data["rating_value"] <= rating_filter[1])
            ]

    if apply_age_limit and age_limit:
        filtered_data = filtered_data[filtered_data["age_limit"].isin(age_limit)]

    if apply_genre and genres:
        filtered_data = filtered_data[
            filtered_data["genre1"].isin(genres) |
            filtered_data["genre2"].isin(genres) |
            filtered_data["genre3"].isin(genres)
            ]

    if apply_director and directors:
        filtered_data = filtered_data[filtered_data["director_name"].isin(directors)]

    if apply_keyword and keywords:
        filtered_data = filtered_data[
            filtered_data["genre1"].isin(keywords) |
            filtered_data["genre2"].isin(keywords) |
            filtered_data["genre3"].isin(keywords)
            ]

# SORTING
if sort_option == "Rating (Highest-Lowest)":
    filtered_data = filtered_data.sort_values("rating_value", ascending=False)
elif sort_option == "Rating (Lowest-Highest)":
    filtered_data = filtered_data.sort_values("rating_value", ascending=True)
elif sort_option == "Alphabetical (A-Z)":
    filtered_data = filtered_data.sort_values("title", ascending=True)
elif sort_option == "Alphabetical (Z-A)":
    filtered_data = filtered_data.sort_values("title", ascending=False)
elif sort_option == "Release Year (Newest-Oldest)":
    filtered_data = filtered_data.sort_values("release_date", ascending=False)
elif sort_option == "Release Year (Oldest-Newest)":
    filtered_data = filtered_data.sort_values("release_date", ascending=True)

# RATINGS WERE IN WRONG FORMAT
filtered_data["rating_value"] = filtered_data["rating_value"].apply(lambda x: f"{x:.1f}")


# APP SIZE
st.html("""
    <style>
        .stMainBlockContainer {
            max-width: 85rem;   
        }
        
        .stSlider, .stMultiSelect, .stSelectbox {
            max-width: 50rem;
        }
    </style>
    """
        )
# SHOWS REQUESTED MOVIES
st.subheader("Movies")
if filtered_data.empty:
    st.write("No movies found with the selected filters.")
else:
    st.markdown(filtered_data.style.hide(axis="index").to_html(), unsafe_allow_html=True)
