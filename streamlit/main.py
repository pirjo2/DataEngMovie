import streamlit as st
import pandas as pd
import duckdb
import numpy as np
#from st_aggrid import AgGrid, GridOptionsBuilder, JsCode #(pip install streamlit-aggrid)

st.image("camera.png")
st.title('Trends in Cinema')

# PROOVI ANDMED
@st.cache_data
def load_data():
    try:
        conn = duckdb.connect(database='./star_schema2.db')

        # Create a SQL query to fetch relevant movie data from the star schema
        query = """
            SELECT m.title, m.release_year, f.rating_value AS rating, 
                   d.name AS director, g.genre1, g.genre2, g.genre3, 
                   k.keyword1, k.keyword2, k.keyword3, a.name AS actor, 
                   d.category, m.overview
            FROM Search_fact m
            JOIN Search_fact f ON m.id = f.movie_ID
            JOIN Search_Crew_bridge scb ON scb.movie_ID = m.id
            JOIN Crew_dimension d ON d.id = scb.crewmate_ID AND scb.job = 'Director'
            JOIN Genre_dimension g ON g.id = f.genre_ID
            JOIN Keyword_dimension k ON k.id = f.keyword_ID
            JOIN Search_Cast_bridge scb_actor ON scb_actor.movie_ID = m.id
            JOIN Cast_dimension a ON a.id = scb_actor.actor_ID
            JOIN Date_dimension dt ON dt.id = f.release_date_ID
        """

        q2 = """SHOW TABLES;"""
        data = conn.execute(q2).fetchdf()
        conn.close()
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        raise e

data = load_data()

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
    apply_category = st.checkbox("Filter by category")
    apply_director = st.checkbox("Filter by director")
    apply_genre = st.checkbox("Filter by genre")
    apply_tag = st.checkbox("Filter by tag")

    if apply_release_year:
        filters_applied = True
        release_year_range = st.slider(
            "Release Year",
            int(data["release_year"].min()),
            int(data["release_year"].max()),
            (2000, 2020)
        )

    if apply_rating:
        filters_applied = True
        rating_filter = st.slider("Rating", 0.0, 10.0, (0.0, 10.0))

    if apply_age_limit:
        filters_applied = True
        age_limit = st.multiselect("Age limit", data["category"].unique())
    else:
        age_limit = []

    if apply_category:
        filters_applied = True
        category = st.multiselect("Category", data["category"].unique())
    else:
        category = []

    if apply_director:
        filters_applied = True
        directors = st.multiselect("Director", data["director"].unique())
    else:
        directors = []

    if apply_genre:
        filters_applied = True
        genres = st.multiselect("Genre", data[["genre1", "genre2", "genre3"]].stack().unique())
    else:
        genres = []

    if apply_tag:
        filters_applied = True
        tags = st.multiselect("Tag", data[["keyword1", "keyword2", "keyword3"]].stack().unique())
    else:
        tags = []

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
            (filtered_data["release_year"] >= release_year_range[0]) &
            (filtered_data["release_year"] <= release_year_range[1])
            ]

    if apply_rating:
        filtered_data = filtered_data[
            (filtered_data["rating"] >= rating_filter[0]) &
            (filtered_data["rating"] <= rating_filter[1])
            ]

    if apply_age_limit and age_limit:
        filtered_data = filtered_data[filtered_data["category"].isin(age_limit)]

    if apply_category and category:
        filtered_data = filtered_data[filtered_data["category"].isin(category)]

    if apply_director and directors:
        filtered_data = filtered_data[filtered_data["director"].isin(directors)]

    if apply_genre and genres:
        filtered_data = filtered_data[filtered_data[["genre1", "genre2", "genre3"]].isin(genres).any(axis=1)]

    if apply_tag and tags:
        filtered_data = filtered_data[
            filtered_data[["keyword1", "keyword2", "keyword3"]].isin(tags).any(axis=1)
        ]

# SORTING
if sort_option == "Rating (Highest-Lowest)":
    filtered_data = filtered_data.sort_values("rating", ascending=False)
elif sort_option == "Rating (Lowest-Highest)":
    filtered_data = filtered_data.sort_values("rating", ascending=True)
elif sort_option == "Alphabetical (A-Z)":
    filtered_data = filtered_data.sort_values("title", ascending=True)
elif sort_option == "Alphabetical (Z-A)":
    filtered_data = filtered_data.sort_values("title", ascending=False)
elif sort_option == "Release Year (Newest-Oldest)":
    filtered_data = filtered_data.sort_values("release_year", ascending=False)
elif sort_option == "Release Year (Oldest-Newest)":
    filtered_data = filtered_data.sort_values("release_year", ascending=True)

# RATINGS WERE IN WRONG FORMAT
filtered_data["rating"] = filtered_data["rating"].apply(lambda x: f"{x:.1f}")

# APP SIZE
st.html("""<style>
    .stMainBlockContainer {
        max-width: 85rem;   
    }
    .stSlider, .stMultiSelect, .stSelectbox {
        max-width: 50rem;
    }
</style>""")

# SHOWS REQUESTED MOVIES
st.subheader("Movies")
if filtered_data.empty:
    st.write("No movies found with the selected filters.")
else:
    st.markdown(filtered_data.style.hide(axis="index").to_html(), unsafe_allow_html=True)
