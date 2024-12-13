"""DAG that runs a streamlit app."""
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration
import os
# Define DAG
with DAG(
        dag_id='KKKKKKKKKKKrun_streamlit_dag',
        start_date=datetime(2023, 12, 1),
        schedule_interval=None,  # Manual runs
        catchup=False,
        dagrun_timeout=duration(hours=1),
        description="DAG to run a Streamlit app.",
        tags=["streamlit", "reporting"]
) as dag:
    # Task to run the Streamlit app
    run_streamlit_script = BashOperator(
        task_id="run_streamlit_app",
        bash_command="streamlit run main.py --server.address=0.0.0.0 --server.port=8051",
        cwd=f"{os.environ['AIRFLOW_HOME']}/dags/streamlit"
    )
    run_streamlit_script