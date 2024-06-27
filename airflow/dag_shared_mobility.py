"""
DAG to calculate paths of scooters.
Also calculates data marts for Grafana dashboard.

For testing, rebuild docker image and run a test like the below example:
    $ docker buildx build -f Dockerfile.etl -t fbardos/shared_mobility:latest . && \
      airflow tasks test -m shared_mobility_wthur mart_trip_distance 2023-08-01

"""
import datetime as dt
import logging
import os
from dataclasses import dataclass
import itertools

from python_docker_operator.operator import PythonDockerOperator

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from airflow.models import Variable

from sharedmobility.config import SharedMobilityConfig


# ################################################################################################################
# DAG
# ################################################################################################################
with DAG(
    dag_id='shared_mobility',
    schedule='5 2 * * *',  # every day at 02:00
    start_date=dt.datetime(2022, 10, 16),
    end_date=dt.datetime(2023, 11, 15),
    max_active_runs=1,
    concurrency=12,
    catchup=True,  # build for every day in the past
    default_args=dict(
        email=Variable.get('AIRFLOW__MAIL_FAILURE'),
        email_on_failure=True,
    ),
    tags=['mart', 'shared_mobility'],
) as dag:

    DB_CONNECTIONS = [
        'psql_marts',
        'psql_public',
    ]

    CITY_CONFIGS = [
        'WinterthurSharedMobilityConfig',
        'ZurichSharedMobilityConfig',
        'UsterSharedMobilityConfig',
        'EffretikonSharedMobilityConfig',
        'FrauenfeldSharedMobilityConfig',
        'StGallenSharedMobilityConfig',
        'BaselSharedMobilityConfig',
        'BielSharedMobilityConfig',
        'GrenchenSharedMobilityConfig',
        'BernSharedMobilityConfig',
    ]

    # Global Settings
    CONFIG = SharedMobilityConfig()
    DOCKER_IMAGE = 'fbardos/shared_mobility:latest'

    t_begin_etl = DummyOperator(
        task_id='begin_etl',
        depends_on_past=True,
    )
    t_end_etl = DummyOperator(
        task_id='end_etl',
        trigger_rule='none_failed'
    )

    # ETL Path ###################################################################################################
    t_assert_table_path = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_path',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_path'], DB_CONNECTIONS)),
        )
    )

    t_path_etl = (
        PythonDockerOperator
        .partial(
            task_id='path_etl',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'path_etl.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                CONFIG.redis_conn_id,
                *DB_CONNECTIONS,
            ],
            skip_exit_code=99,  # Custom exit code for skipping
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )
    
    # ETL Provider ###############################################################################################
    t_assert_table_provider = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_provider',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_provider'], DB_CONNECTIONS)),
        )
    )

    t_provider_etl = (
        PythonDockerOperator
        .partial(
            task_id='provider_etl',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'path_provider.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                *DB_CONNECTIONS,
            ],
            skip_exit_code=99,  # Custom exit code for skipping
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )
    
    # ETL IDs ####################################################################################################
    t_assert_table_ids = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_ids',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_ids'], DB_CONNECTIONS)),
        )
    )


    t_ids_etl = (
        PythonDockerOperator
        .partial(
            task_id='provider_ids',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'ids_etl.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                *DB_CONNECTIONS,
            ],
            skip_exit_code=99,  # Custom exit code for skipping
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )

    # ASSERT ROWS ################################################################################################
    t_assert_rows_path = (
        PythonDockerOperator
        .partial(
            task_id='assert_rows_path',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_rows.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, ['--failifnorows'])),
        )
    )
    
    t_begin_calculate_marts = DummyOperator(
        task_id='begin_calculate_marts'
    )

    # MART EDGES #################################################################################################
    t_assert_table_mart_edges = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_edges',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_mart_edges'], DB_CONNECTIONS)),
        )
    )
    
    t_mart_edges = (
        PythonDockerOperator
        .partial(
            task_id='mart_edges',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_edges.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )

    # MART DISTINCT IDS ##########################################################################################
    t_assert_table_mart_distinct_ids = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_distinct_ids',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_mart_distinct_ids'], DB_CONNECTIONS)),
        )
    )

    t_mart_distinct_ids = (
        PythonDockerOperator
        .partial(
            task_id='mart_distinct_ids',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_distinct_ids.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )
    

    # MART SCOOTER AGE ###########################################################################################
    t_assert_table_mart_scooter_age = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_scooter_age',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_mart_scooter_age'], DB_CONNECTIONS)),
        )
    )

    t_mart_scooter_age = (
        PythonDockerOperator
        .partial(
            task_id='mart_scooter_age',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_scooter_age.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )

    # MART TRIP DISTANCE #########################################################################################
    t_assert_table_mart_trip_distance = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_trip_distance',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(['shared_mobility_mart_trip_distance'], DB_CONNECTIONS)),
        )
    )
    
    t_mart_trip_distance = (
        PythonDockerOperator
        .partial(
            task_id='mart_trip_distance',
            docker_url=Variable.get('DOCKER__URL'),
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_trip_distance.py'),
            custom_connection_ids=DB_CONNECTIONS,
        )
        .expand(
            custom_cmd_args=list(itertools.product(DB_CONNECTIONS, CITY_CONFIGS)),
        )
    )
    
    # CHAIN ###################################################################################################
    chain(
        t_begin_etl,
        [
            t_assert_table_provider,
            t_assert_table_ids,
            t_assert_table_path,
        ],
        [
            t_provider_etl,
            t_ids_etl,
            t_path_etl,
        ],
        t_end_etl,
        t_assert_rows_path,
        t_begin_calculate_marts,
        [
            t_assert_table_mart_edges,
            t_assert_table_mart_distinct_ids,
            t_assert_table_mart_scooter_age,
            t_assert_table_mart_trip_distance,
        ],
        [
            t_mart_edges,
            t_mart_distinct_ids,
            t_mart_scooter_age,
            t_mart_trip_distance,
        ]
    )


# Easily test DAG with a debugger
# For example, start with:
# docker buildx build -f Dockerfile.etl -t fbardos/shared_mobility:latest . && python -m ipdb airflow/shared_mobility_wthur.py
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    dag.test(execution_date=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc))
