from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='restart_docker_container',
        default_args=default_args,
        description='DAG to restart a Docker container',
        schedule_interval=None,  # Run manually
        start_date=datetime(2024, 12, 1),
        catchup=False,
) as dag:

    restart_container = DockerOperator(
        task_id='restart_container',
        image='alpine',
        api_version='auto',
        command='docker restart streamlit',
        docker_url='unix://var/run/docker.sock',  # Default socket URL for Docker
    )
