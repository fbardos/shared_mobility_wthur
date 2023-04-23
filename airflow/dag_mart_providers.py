import pandas as pd
import datetime as dt
from dateutil import tz

from sqlalchemy import MetaData, DateTime, Table, Column, Integer, Float, String

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import get_current_context

from great_expectations.dataset import PandasDataset

with DAG(
    dag_id='shared_mobility_mart_providers',
    schedule_interval='0 2 * * *',  # every day at 02:00
    start_date=dt.datetime(2022, 10, 15),
    max_active_runs=1,
    catchup=True,  # build for every day in the past
    tags=['mart', 'shared_mobility'],
) as dag:

    # Constants
    WTHUR_SOUTH = 47.449753
    WTHUR_NORTH = 47.532571
    WTHUR_WEST = 8.687158
    WTHUR_EAST = 8.784503
    psql_conn_id = 'psql_marts'
    mongo_conn_id = 'mongo_opendata'
    target_table = 'mart_shared_mobility_providers'

    # Task 1: Assert that target table is available
    @task(task_id='assert_table')
    def assert_table():

        # SQLAlchemy ORM: table definition
        table_name = target_table
        metadata_obj = MetaData()
        tbl_providers = Table(
            table_name,
            metadata_obj,
            Column('time', DateTime, primary_key=True, nullable=False),
            Column('provider', String, primary_key=True, nullable=False),
            Column('count_datapoints', Integer, nullable=False),
            Column('count_distinct_ids', Integer, nullable=False),
            Column('avg_delta_updated', Float, nullable=False),
            Column('count_available', Integer, nullable=False),
            Column('count_disabled', Integer, nullable=False),
            Column('count_reserved', Integer, nullable=False),
        )

        hook = PostgresHook(psql_conn_id)
        engine = hook.get_sqlalchemy_engine()

        # If table is not existent, create table
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, table_name):
                tbl_providers.create(engine)



    # Task 2: Load from Mongo, build mart and write to Postgres
    @task(task_id='etl')
    def etl():
        airflow_context = get_current_context()
        execution_date = airflow_context.get('execution_date')
        assert isinstance(execution_date, dt.datetime)

        mongo_hook = MongoHook(mongo_conn_id)
        with mongo_hook.get_conn() as client:
            start = dt.datetime(execution_date.year, execution_date.month, execution_date.day, tzinfo=tz.tzutc())
            end = dt.datetime(execution_date.year, execution_date.month, execution_date.day, 23, 59, 59, tzinfo=tz.tzutc())
            query = {
                'geometry.coordinates.1': {'$gt': WTHUR_SOUTH, '$lt': WTHUR_NORTH},
                'geometry.coordinates.0': {'$gt': WTHUR_WEST, '$lt': WTHUR_EAST},
                '_meta_runtime_utc': {'$gt': start, '$lt': end},
            }
            db = client.opendata
            col = db.shared_mobility

            cursor = col.find(query)
            df = pd.DataFrame(list(cursor))

        # Transform
        df = (df
            .assign(provider=lambda x: x['properties'].apply(lambda y: y['provider']['name']))
            .assign(delta_updated=lambda x: x['_meta_runtime_utc'] - x['_meta_last_updated_utc'])
            .assign(id=lambda x: x['properties'].apply(lambda y: y['id']))
            .assign(available=lambda x: x['properties'].apply(lambda y: y['available']))
            .assign(disabled=lambda x: x['properties'].apply(lambda y: y.get('vehicle', {}).get('status', {}).get('disabled')))
            .assign(reserved=lambda x: x['properties'].apply(lambda y: y.get('vehicle', {}).get('status', {}).get('reserved')))
            .groupby(['provider', pd.Grouper(key='_meta_runtime_utc', freq='2min')])
            .agg(
                count_datapoints=('_meta_runtime_utc', 'size'),
                count_distinct_ids=('id', lambda x: x.nunique()),
                avg_delta_updated=('delta_updated', 'mean'),
                count_available=('available', 'sum'),
                count_disabled=('disabled', 'sum'),
                count_reserved=('reserved', 'sum'),
            )
            .reset_index()
            .rename(columns={'_meta_runtime_utc': 'time'})
        )

        # QA
        dfqa = PandasDataset(df)
        assert dfqa.expect_column_values_to_be_between('count_datapoints', 0, 700).success
        assert dfqa.expect_column_values_to_be_between('count_distinct_ids', 0, 700).success

        # Load to PSQL
        psql_hook = PostgresHook(psql_conn_id)
        engine = psql_hook.get_sqlalchemy_engine()
        df.to_sql(target_table, engine, index=False, if_exists='append')


    t1 = assert_table()
    t2 = etl()
    t1.set_downstream(t2)
