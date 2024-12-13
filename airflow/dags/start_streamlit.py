from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime
import os

streamlit_dir = f"{os.environ['AIRFLOW_HOME']}/dags/streamlit"

# Define DAG
with DAG(
        dag_id='start_streamlit',
        start_date=datetime(2023, 12, 1),
        schedule_interval=None,
        catchup=False,
        description="DAG to start Streamlit app.",
) as dag:

    start_streamlit = BashOperator(
        task_id="start_streamlit_app",
        bash_command=(
            f"pip install streamlit && "
            f"cd {streamlit_dir} && "
            "streamlit run main.py --server.address=0.0.0.0 --server.port=8051" # 0.0.0.0  will hopefully be replaced
        )
    )

    start_streamlit