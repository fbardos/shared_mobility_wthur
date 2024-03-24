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

from python_docker_operator.operator import PythonDockerOperator

from airflow import DAG
from airflow.operators.dummy import DummyOperator


@dataclass(frozen=True)
class SharedMobilityConfig:
    pos_south: float = 47.449753
    pos_north: float = 47.532571
    pos_west: float = 8.687158
    pos_east: float = 8.784503
    pos_change_when_bigger_than_meter: int = 50
    mongo_conn_id: str = 'mongo_opendata'
    redis_conn_id: str = 'redis_cache'
    conn_id_private: str = 'psql_marts'
    conn_id_public: str = 'psql_public'
    keep_public_days: int = 365


# ################################################################################################################
# DAG
# ################################################################################################################

with DAG(
    dag_id='shared_mobility_wthur',
    schedule='5 2 * * *',  # every day at 02:00
    start_date=dt.datetime(2022, 10, 16),
    max_active_runs=1,
    catchup=True,  # build for every day in the past
    tags=['mart', 'shared_mobility'],
) as dag:

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
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[CONFIG.conn_id_private, CONFIG.conn_id_public],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_path', CONFIG.conn_id_private],
            ['shared_mobility_path', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_path.set_upstream(t_begin_etl)

    t_path_etl = (
        PythonDockerOperator
        .partial(
            task_id='path_etl',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'path_etl.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                CONFIG.redis_conn_id,
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_path_etl.set_upstream(t_assert_table_path)
    
    t_path_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='path_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_path',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_path',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_path_delete_old.set_upstream(t_path_etl)
    t_path_delete_old.set_downstream(t_end_etl)

    # ETL Provider ###############################################################################################
    t_assert_table_provider = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_provider',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_provider', CONFIG.conn_id_private],
            ['shared_mobility_provider', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_provider.set_upstream(t_begin_etl)

    t_provider_etl = (
        PythonDockerOperator
        .partial(
            task_id='provider_etl',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'path_provider.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_provider_etl.set_upstream(t_assert_table_provider)
    
    t_provider_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='provider_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_provider',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_provider',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_provider_delete_old.set_upstream(t_provider_etl)
    t_provider_delete_old.set_downstream(t_end_etl)
    
    # ETL IDs ####################################################################################################
    t_assert_table_ids = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_ids',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_ids', CONFIG.conn_id_private],
            ['shared_mobility_ids', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_ids.set_upstream(t_begin_etl)

    t_ids_etl = (
        PythonDockerOperator
        .partial(
            task_id='provider_ids',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'ids_etl.py'),
            custom_connection_ids=[
                CONFIG.mongo_conn_id,
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_ids_etl.set_upstream(t_assert_table_ids)

    t_ids_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='ids_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_ids',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_ids',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_ids_delete_old.set_upstream(t_ids_etl)
    t_ids_delete_old.set_downstream(t_end_etl)

    # ASSERT ROWS ################################################################################################
    t_assert_rows_path = (
        PythonDockerOperator
        .partial(
            task_id='assert_rows_path',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [
                CONFIG.conn_id_private,
                '--failifnorows',
            ],
            [
                CONFIG.conn_id_public,
                '--failifnorows',
            ],
        ])
    )
    t_assert_rows_path.set_upstream(t_end_etl)
    
    
    t_begin_calculate_marts = DummyOperator(
        task_id='begin_calculate_marts'
    )
    t_begin_calculate_marts.set_upstream(t_assert_rows_path)

    # MART EDGES #################################################################################################
    t_assert_table_mart_edges = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_edges',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[CONFIG.conn_id_private, CONFIG.conn_id_public],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_mart_edges', CONFIG.conn_id_private],
            ['shared_mobility_mart_edges', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_mart_edges.set_upstream(t_begin_calculate_marts)

    t_mart_edges = (
        PythonDockerOperator
        .partial(
            task_id='mart_edges',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_edges.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_mart_edges.set_upstream(t_assert_table_mart_edges)

    t_mart_edges_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='mart_edges_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_mart_edges',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_mart_edges',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_mart_edges_delete_old.set_upstream(t_mart_edges)

    # MART DISTINCT IDS ##########################################################################################
    t_assert_table_mart_distinct_ids = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_distinct_ids',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[CONFIG.conn_id_private, CONFIG.conn_id_public],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_mart_distinct_ids', CONFIG.conn_id_private],
            ['shared_mobility_mart_distinct_ids', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_mart_distinct_ids.set_upstream(t_begin_calculate_marts)

    t_mart_distinct_ids = (
        PythonDockerOperator
        .partial(
            task_id='mart_distinct_ids',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_distinct_ids.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_mart_distinct_ids.set_upstream(t_assert_table_mart_distinct_ids)

    t_mart_distinct_ids_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='mart_distinct_ids_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_mart_distinct_ids',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_mart_distinct_ids',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_mart_distinct_ids_delete_old.set_upstream(t_mart_distinct_ids)

    # MART SCOOTER AGE ###########################################################################################
    t_assert_table_mart_scooter_age = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_scooter_age',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[CONFIG.conn_id_private, CONFIG.conn_id_public],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_mart_scooter_age', CONFIG.conn_id_private],
            ['shared_mobility_mart_scooter_age', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_mart_scooter_age.set_upstream(t_begin_calculate_marts)

    t_mart_scooter_age = (
        PythonDockerOperator
        .partial(
            task_id='mart_scooter_age',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_scooter_age.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_mart_scooter_age.set_upstream(t_assert_table_mart_scooter_age)

    t_mart_scooter_age_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='mart_scooter_age_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_mart_scooter_age',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_mart_scooter_age',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_mart_scooter_age_delete_old.set_upstream(t_mart_scooter_age)
    
    # MART TRIP DISTANCE #########################################################################################
    t_assert_table_mart_trip_distance = (
        PythonDockerOperator
        .partial(
            task_id='assert_table_mart_trip_distance',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'assert_table.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            ['shared_mobility_mart_trip_distance', CONFIG.conn_id_private],
            ['shared_mobility_mart_trip_distance', CONFIG.conn_id_public],
        ])
    )
    t_assert_table_mart_trip_distance.set_upstream(t_begin_calculate_marts)
    
    t_mart_trip_distance = (
        PythonDockerOperator
        .partial(
            task_id='mart_trip_distance',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'mart_trip_distance.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(custom_cmd_args=[
            [CONFIG.conn_id_private],
            [CONFIG.conn_id_public],
        ])
    )
    t_mart_trip_distance.set_upstream(t_assert_table_mart_trip_distance)

    t_mart_trip_distance_delete_old = (
        PythonDockerOperator
        .partial(
            task_id='mart_trip_distance_delete_old',
            image=DOCKER_IMAGE,
            custom_file_path=os.path.join('tasks', 'delete_old_rows.py'),
            custom_connection_ids=[
                CONFIG.conn_id_private,
                CONFIG.conn_id_public
            ],
        )
        .expand(
            custom_cmd_args=[
                [
                    'shared_mobility_mart_trip_distance',
                    'time_to',
                    CONFIG.conn_id_public,
                    '--isdelete',
                    '--deletebeforedays', 365
                ],
                [
                    'shared_mobility_mart_trip_distance',
                    'time_to',
                    CONFIG.conn_id_private,
                ],
            ]
        )
    )
    t_mart_trip_distance_delete_old.set_upstream(t_mart_trip_distance)


# Easily test DAG with a debugger
# For example, start with:
# docker buildx build -f Dockerfile.etl -t fbardos/shared_mobility:latest . && python -m ipdb airflow/shared_mobility_wthur.py
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    dag.test(execution_date=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc))
