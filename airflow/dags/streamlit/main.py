import streamlit as st
import duckdb
import pandas as pd

st.image("camera.png")
st.title('Trends in Cinema')

@st.cache_data
def execute_query(query: str):
    conn = duckdb.connect(database="/opt/airflow/star_schema.db", read_only=True)
    return conn.execute(query).fetchall()

if "show_questions" not in st.session_state:
    st.session_state.show_questions = False

if st.button("Show/Hide Questions"):
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
    st.write(result)
    st.markdown('##')
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
    LIMIT 1;
    """
    result = execute_query(query_2)
    st.markdown("**Query Result:**")
    st.write(result[0][0])
    st.markdown('##')
    st.write("3: Which director has on average the highest rated movies?")
    query_3 = """
    SELECT 
        CRD.name AS director_name, 
        AVG(SF.rating_value) AS avg_rating
    FROM 
        Search_fact SF
    JOIN 
        Search_Crew_bridge SCB ON SF.crew_ID = SCB.crewmate_ID
    JOIN 
        Crew_dimension CRD ON SCB.crewmate_ID = CRD.id
    WHERE 
        SCB.job = 'Director' 
        AND SF.rating_value IS NOT NULL
    GROUP BY 
        CRD.name
    ORDER BY 
        avg_rating DESC
    LIMIT 1;
    """
    result = execute_query(query_3)
    st.markdown("**Query Result:**")
    st.write(result[0][0])
    st.markdown('##')
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
    st.write(result[0][0])

