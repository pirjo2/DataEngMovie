from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'YOUUUUUUrun_streamlit_app',
    default_args=default_args,
    description='Run a Streamlit app',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=datetime(2024, 12, 11),
    catchup=False,
)

# Define the task to run the Streamlit app
run_streamlit_app = BashOperator(
    task_id='run_streamlit_app',
    bash_command='pwd && echo $YOUR_ENV_VAR && cd /airflow/data/streamlit && streamlit run main.py',
    dag=dag,
)