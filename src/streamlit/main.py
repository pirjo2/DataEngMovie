import streamlit as st
import pandas as pd
import numpy as np
#from st_aggrid import AgGrid, GridOptionsBuilder, JsCode #(pip install streamlit-aggrid)

st.image("camera.png")
st.title('Trends in Cinema')

# PROOVI ANDMED
@st.cache_data
def load_data():
    data = pd.DataFrame({
        "Title": ["B", "A", "D", "C"],
        "Release Year": [2000, 2010, 2020, 2005],
        "Rating": [5.0, 4.2, 3.1, 1.4],
        "Director": ["Director 1", "Director 2", "Director 1", "Director 3"],
        "Genre": ["Action", "Drama", "Action", "Comedy"],
        "Tag": ["Epic", "Sad", "Thrilling", "Funny"],
        "Age limit": ["G", "PG", "PG-13", "R"],
        "Category": ["Christmas", "Thanksgiving", "New Years", "Spring"],
        "Description": [
            "An epic tale of adventure and bravery.",
            "A heartwarming drama that explores the depths of human emotions.",
            "A thrilling action film with stunning visuals.",
            "A comedy that will leave you in stitches."
        ],
    })
    return data

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
    apple_category = st.checkbox("Filter by category")
    apply_director = st.checkbox("Filter by director")
    apply_genre = st.checkbox("Filter by genre")
    apply_tag = st.checkbox("Filter by tag")

    if apply_release_year:
        filters_applied = True
        release_year_range = st.slider(
            "Release Year",
            int(data["Release Year"].min()),
            int(data["Release Year"].max()),
            (2000, 2020)
        )

    if apply_rating:
        filters_applied = True
        rating_filter = st.slider("Rating", 0.0, 10.0, (0.0, 10.0))

    if apply_age_limit:
        filters_applied = True
        age_limit = st.multiselect("Age limit", data["Age limit"].unique())
    else:
        age_limit = []

    if apple_category:
        filters_applied = True
        category = st.multiselect("Category", data["Category"].unique())
    else:
        category = []

    if apply_director:
        filters_applied = True
        directors = st.multiselect("Director", data["Director"].unique())
    else:
        directors = []

    if apply_genre:
        filters_applied = True
        genres = st.multiselect("Genre", data["Genre"].unique())
    else:
        genres = []

    if apply_tag:
        filters_applied = True
        tags = st.multiselect("Tag", data["Tag"].unique())
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
            (filtered_data["Release Year"] >= release_year_range[0]) &
            (filtered_data["Release Year"] <= release_year_range[1])
            ]

    if apply_rating:
        filtered_data = filtered_data[
            (filtered_data["Rating"] >= rating_filter[0]) &
            (filtered_data["Rating"] <= rating_filter[1])
            ]

    if apply_age_limit and age_limit:
        filtered_data = filtered_data[filtered_data["Age limit"].isin(age_limit)]

    if apple_category and category:
        filtered_data = filtered_data[filtered_data["Category"].isin(category)]

    if apply_director and directors:
        filtered_data = filtered_data[filtered_data["Director"].isin(directors)]

    if apply_genre and genres:
        filtered_data = filtered_data[filtered_data["Genre"].isin(genres)]

    if apply_tag and tags:
        filtered_data = filtered_data[filtered_data["Tag"].isin(tags)]

# SORTING
if sort_option == "Rating (Highest-Lowest)":
    filtered_data = filtered_data.sort_values("Rating", ascending=False)
elif sort_option == "Rating (Lowest-Highest)":
    filtered_data = filtered_data.sort_values("Rating", ascending=True)
elif sort_option == "Alphabetical (A-Z)":
    filtered_data = filtered_data.sort_values("Title", ascending=True)
elif sort_option == "Alphabetical (Z-A)":
    filtered_data = filtered_data.sort_values("Title", ascending=False)
elif sort_option == "Release Year (Newest-Oldest)":
    filtered_data = filtered_data.sort_values("Release Year", ascending=False)
elif sort_option == "Release Year (Oldest-Newest)":
    filtered_data = filtered_data.sort_values("Release Year", ascending=True)

# RATINGS WERE IN WRONG FORMAT
filtered_data["Rating"] = filtered_data["Rating"].apply(lambda x: f"{x:.1f}")


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
