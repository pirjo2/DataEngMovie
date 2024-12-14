from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

# Define the DAG
with DAG(
        dag_id='stop_streamlit',
        start_date=datetime(2024, 12, 1),
        schedule_interval=None,
        catchup=False,
        description="DAG to stop Streamlit app.",
) as dag:

    # Task to stop Streamlit app
    stop_streamlit = BashOperator(
        task_id="stop_streamlit_app",
        bash_command="pkill -f 'streamlit run main.py'"
    )

    stop_streamlit