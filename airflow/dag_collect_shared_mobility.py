import datetime as dt
import os

from airflow import DAG

from python_docker_operator.operator import PythonDockerOperator


with DAG(
    dag_id='gather_shared_mobility',
    schedule_interval='*/2 * * * *',
    start_date=dt.datetime(2022, 9, 1),
    max_active_runs=1,
    catchup=False,
    tags=['gather', 'shared_mobility'],
) as dag:

    DOCKER_IMAGE = 'fbardos/shared_mobility:latest'

    t1 = PythonDockerOperator(
        task_id='gather_data',
        image=DOCKER_IMAGE,
        custom_file_path=os.path.join('tasks', 'extract.py'),
        custom_connection_ids=['mongo_opendata'],
        custom_cmd_args=['mongo_opendata'],
    )

    t1

