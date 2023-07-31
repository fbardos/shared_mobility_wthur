"""
DAG to calculate paths of scooters.
Also calculates data marts for Grafana dashboard.

"""
import datetime as dt
import logging

import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
from dataclasses import dataclass
from dateutil import tz
from geoalchemy2 import Geometry
from great_expectations.dataset import PandasDataset
from psycopg2 import sql
from shapely.geometry import LineString, Point
from sqlalchemy import (Boolean, Column, DateTime, Float, Index, Integer,
                        MetaData, String, Table, delete)
from typing import Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Cannot use sqlalchemy.orm.declarative_base() in the airflow context.
# Otherwise, the same MetaData object as for the metadatabase of airflow is used.
# This creates many conflicts during DagBag filling.
meta = MetaData()

table_path_cols = (
    Column('id', String, primary_key=True, nullable=False),
    Column('provider', String, nullable=False, index=True),
    Column('point', Geometry('POINT'), nullable=False),
    Column('time_from', DateTime, primary_key=True, nullable=False, index=True),
    Column('time_to', DateTime, nullable=False, index=True),
    Column('distance_m', Float, nullable=False),
    Column('moving', Boolean, nullable=False),
    Column('distance_m_walk', Float, nullable=True),
    Column('path_walk_since_last', Geometry('LINESTRING'), nullable=True),
    Column('distance_m_bike', Float, nullable=True),
    Column('path_bike_since_last', Geometry('LINESTRING'), nullable=True),
)
table_path = Table(
    'shared_mobility_path', meta,
    *(i.copy() for i in table_path_cols)
)
table_path_tmp = Table(
    'shared_mobility_path_tmp', meta,
    *(i.copy() for i in table_path_cols),
    prefixes=['TEMPORARY']
)

table_provider = Table(
    'shared_mobility_provider', meta,
    Column('time', DateTime, primary_key=True, nullable=False),
    Column('provider', String, primary_key=True, nullable=False),
    Column('count_datapoints', Integer, nullable=False),
    Column('count_distinct_ids', Integer, nullable=False),
    Column('avg_delta_updated', Float, nullable=False),
    Column('count_available', Integer, nullable=False),
    Column('count_disabled', Integer, nullable=False),
    Column('count_reserved', Integer, nullable=False),
)

table_id_cols = (
    Column('id', String, primary_key=True, nullable=False),
    Column('provider', String, nullable=False, index=True),
    Column('first_seen', DateTime, nullable=False, index=True),
    Column('last_seen', DateTime, nullable=False, index=True),
    Column('datapoints', Integer, nullable=False),
)
table_id = Table(
    'shared_mobility_ids', meta,
    *(i.copy() for i in table_id_cols)
)
table_id_tmp = Table(
    'shared_mobility_ids_tmp', meta,
    *(i.copy() for i in table_id_cols),
    prefixes=['TEMPORARY']
)

@dataclass
class AirflowContextUtils:
    context: Context

    @property
    def data_interval_start(self) -> dt.datetime:
        """Returns dt.datetime with timezone UTC."""
        time = self.context.get('data_interval_start')
        assert isinstance(time, dt.datetime)
        return time

    @property
    def data_interval_end(self) -> dt.datetime:
        """In addition subtracts one microsecond from the original data_interval_end.

        Returns dt.datetime with timezone UTC.
        """
        time = self.context.get('data_interval_end')
        assert isinstance(time, dt.datetime)
        return time - dt.timedelta(microseconds=1)


@dataclass
class SharedMobilityConnectionStrings:
    """ Dataclass for connection strings and config.

    Args:
        - conn_id_private: Airflow connection string for private PostGIS.
        - conn_id_public: Airflow connection string for public PostGIS.
        - keep_public_days: Amount of days for scooter data is kept in public PostGIS.

    """
    conn_id_private: str
    conn_id_public: str
    keep_public_days: int

    @property
    def conn_ids(self) -> Tuple[str, str]:
        "Returns first the private, then the public Airflow connection ID."
        return self.conn_id_private, self.conn_id_public

    def delete_from(self, execution_date: dt.datetime) -> dt.datetime:
        return execution_date - dt.timedelta(days=self.keep_public_days)

    def is_delete(self, context, conn_id: str) -> bool:
        if conn_id == self.conn_id_private:
            return False
        else:
            return True


class CheckOrCreatePostgresTableOperator(BaseOperator):

    def __init__(self, meta: MetaData, table_name: str, target_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self._meta = meta
        self._table_name = table_name
        self._target_conn_id = target_conn_id

    # def execute(self, context, target_conn_id: str):
    def execute(self, context):
        engine = PostgresHook(self._target_conn_id).get_sqlalchemy_engine()

        # If table is not existent, create table
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, self._table_name):
                self._meta.tables[self._table_name].create(engine)


class AssertPathRowsExecutionDate(BaseOperator):

    def __init__(self, target_conn_id: str, fail_if_no_rows: bool = False, **kwargs):
            super().__init__(**kwargs)
            self._target_conn_id = target_conn_id
            self._fail_if_no_rows = fail_if_no_rows

    def execute(self, context):
        context_utils = AirflowContextUtils(get_current_context())
        conn = PostgresHook(self._target_conn_id).get_conn()
        query = (sql.
            SQL("SELECT id from {source} where time_to BETWEEN %(t_start)s and %(t_end)s")
            .format(source=sql.Identifier(table_path.name))
        )
        try:
            cur = conn.cursor()
            cur.execute(query, dict(t_start=context_utils.data_interval_start, t_end=context_utils.data_interval_end))
            if (rows := cur.rowcount) == 0:
                msg = f'Expected rows for path table >0 for a given time interval. Rows: {rows}'
                if self._fail_if_no_rows:
                    raise Exception(msg)
                else:
                    logging.warning(msg)
            else:
                logging.info(f'Found rows for the given time interval in table rows: {rows}')
        finally:
            conn.close()


class DeleteOldRows(BaseOperator):

    def __init__(
        self,
        config: SharedMobilityConnectionStrings,
        target_conn_id: str,
        table_name: str,
        column_name: str,
        **kwargs
    ):
            super().__init__(**kwargs)
            self._config = config
            self._target_conn_id = target_conn_id
            self._table_name = table_name
            self._column_name = column_name

    def execute(self, context):
        if self._config.is_delete(context, self._target_conn_id):
            context_utils = AirflowContextUtils(get_current_context())
            delete_from = self._config.delete_from(context_utils.data_interval_start)
            conn = PostgresHook(self._target_conn_id).get_conn()
            query = (sql.
                SQL("DELETE FROM {table} where {column}::date < %(delete_from)s::date ")
                .format(
                    table=sql.Identifier(self._table_name),
                    column=sql.Identifier(self._column_name),
                )
            )
            try:
                cur = conn.cursor()
                cur.execute(query, dict(delete_from=delete_from))
                logging.log(
                    logging.ERROR if (rows := cur.rowcount) == 0 else logging.INFO,
                    f'Table {self._table_name}: {rows} rows were affected.'
                )
                conn.commit()
            finally:
                conn.close()
        else:
            raise AirflowSkipException



with DAG(
    dag_id='shared_mobility_wthur',
    schedule_interval='5 2 * * *',  # every day at 02:00
    start_date=dt.datetime(2022, 10, 16),
    max_active_runs=1,
    catchup=True,  # build for every day in the past
    tags=['mart', 'shared_mobility'],
) as dag:

    # Constants
    WTHUR_SOUTH = 47.449753
    WTHUR_NORTH = 47.532571
    WTHUR_WEST = 8.687158
    WTHUR_EAST = 8.784503
    POS_CHANGE_WHEN_BIGGER_THAN_METER = 50
    MONGO_CONN_ID = 'mongo_opendata'
    CONFIG = SharedMobilityConnectionStrings(
        conn_id_private='psql_marts',
        conn_id_public='psql_public',
        keep_public_days=365,
    )


    # ################################################################################################################
    # SUBDAG: Provider
    # ################################################################################################################

    # Task: Load from Mongo, build mart and write to Postgres
    @task(task_id='provider_etl', retries=1, retry_delay=dt.timedelta(minutes=5))
    def provider_etl(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        mongo_hook = MongoHook(MONGO_CONN_ID)
        with mongo_hook.get_conn() as client:
            query = {
                'geometry.coordinates.1': {'$gt': WTHUR_SOUTH, '$lt': WTHUR_NORTH},
                'geometry.coordinates.0': {'$gt': WTHUR_WEST, '$lt': WTHUR_EAST},
                '_meta_runtime_utc': {'$gt': context_utils.data_interval_start, '$lt': context_utils.data_interval_end},
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
            .assign(available=lambda x: (
                x['properties']
                .apply(lambda y: y['available'])
                .fillna(0)
                .astype(int)
            ))
            .assign(disabled=lambda x: (
                x['properties']
                .apply(lambda y: y.get('vehicle', {}).get('status', {}).get('disabled'))
                .fillna(0)
                .astype(int)
            ))
            .assign(reserved=lambda x: (
                x['properties']
                .apply(lambda y: y.get('vehicle', {}).get('status', {}).get('reserved'))
                .fillna(0)
                .astype(int)
            ))
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
        with PostgresHook(target_conn_id).get_sqlalchemy_engine().begin() as conn:
            rows = df.to_sql(table_provider.name, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

    # ################################################################################################################
    # SUBDAG: ID
    # ################################################################################################################
    @task(task_id='id_etl', retries=1, retry_delay=dt.timedelta(minutes=5))
    def id_etl(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        mongo_hook = MongoHook(MONGO_CONN_ID)
        with mongo_hook.get_conn() as client:
            query = {
                'geometry.coordinates.1': {'$gt': WTHUR_SOUTH, '$lt': WTHUR_NORTH},
                'geometry.coordinates.0': {'$gt': WTHUR_WEST, '$lt': WTHUR_EAST},
                '_meta_runtime_utc': {'$gt': context_utils.data_interval_start, '$lt': context_utils.data_interval_end},
            }
            db = client.opendata
            col = db.shared_mobility
            cursor = col.find(query)
            df = pd.DataFrame(list(cursor))

        df = (df
            .assign(id=lambda x: x['properties'].apply(lambda y: y['id']))
            .assign(provider=lambda x: x['properties'].apply(lambda y: y['provider']['name']))
            .assign(time=lambda x: x['_meta_last_updated_utc'])
            .groupby(['id', 'provider'])
            .agg(
                first_seen=('time', 'min'),
                last_seen=('time', 'max'),
                datapoints=('time', 'nunique'),
            )
            .reset_index()
        )

        logging.info(f'Fill the table {table_id.name} with UPSERT')
        with PostgresHook(target_conn_id).get_sqlalchemy_engine().begin() as conn:

            # Create temporary table to insert
            meta.tables[table_id_tmp.name].create(conn)

            # Insert data from one DAG run into temporary table
            rows = df.to_sql(table_id_tmp.name, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

            # Insert to main table (shared_mobility_ids) with UPSERT
            query = f"""
                INSERT INTO {table_id.name} as ins
                SELECT *
                FROM {table_id_tmp.name}
                ON CONFLICT (id) DO UPDATE
                SET
                    provider = EXCLUDED.provider
                    , first_seen = LEAST(ins.first_seen, EXCLUDED.first_seen)
                    , last_seen = GREATEST(ins.last_seen, EXCLUDED.last_seen)
                    , datapoints = ins.datapoints + EXCLUDED.datapoints
                ;
            """
            rows = conn.execute(query)
            logging.log(
                logging.WARNING if rows == 0 else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

    # ################################################################################################################
    # SUBDAG: PATH
    # ################################################################################################################

    # Task: Load from Mongo, build mart and write to Postgres
    @task(task_id='path_etl', retries=1, retry_delay=dt.timedelta(minutes=5))
    def path_etl(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())

        # Create PSQL engine
        psql_hook = PostgresHook(target_conn_id)
        engine = psql_hook.get_sqlalchemy_engine()


        # Load all datapoints for the given day
        logging.info('Load data from MongoDB')
        mongo_hook = MongoHook(MONGO_CONN_ID)
        with mongo_hook.get_conn() as client:
            query = {
                'geometry.coordinates.1': {'$gt': WTHUR_SOUTH, '$lt': WTHUR_NORTH},
                'geometry.coordinates.0': {'$gt': WTHUR_WEST, '$lt': WTHUR_EAST},
                # Datetime is stores as UTC
                '_meta_runtime_utc': {'$gt': context_utils.data_interval_start, '$lt': context_utils.data_interval_end},
            }
            projection = {
                '_id': 0,
                'geometry.coordinates': 1,
                'id': 1,
                '_meta_last_updated_utc': 1,
                'properties.provider.name': 1,
            }
            db = client.opendata
            col = db.shared_mobility

            cursor = col.find(query, projection)
            df = pd.DataFrame(list(cursor))


        # Extract data from MongoDB result set
        logging.info('Transform datapoints')
        df = (df
            .assign(provider=lambda x: x['properties'].apply(lambda y: y['provider']['name']))
            .assign(time=lambda x: x['_meta_last_updated_utc'])
            .assign(point_x=lambda x: x['geometry'].apply(lambda y: y['coordinates'][0]))
            .assign(point_y=lambda x: x['geometry'].apply(lambda y: y['coordinates'][1]))
            .sort_values(by=['id', 'time'])
            [['id', 'provider', 'time', 'point_x', 'point_y']]
        )

        # Load the last 2 rows (t-1, t-2) of every group id and time_to to compare later with
        # the first entry of the DAG execution date (t0).
        # When position t-1 == t0, then the second last point (t-2) is needed to calculate
        # the correct path.
        logging.info('Load additional info for past locations.')
        query = sql.SQL("""
            select *
                from (
                select
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY time_to desc) as r,
                    mart.id,
                    mart.provider,
                    mart.time_from as time,
                    mart.point
                from {mart} mart
                where
                    time_to BETWEEN %(t_start)s - INTERVAL '30 days' AND %(t_start)s
            ) x
            where
                x.r <= 2
        """).format(mart=sql.Identifier(table_path.name))

        with psql_hook.get_conn() as conn:
            gdf_before = (
                gpd.GeoDataFrame.from_postgis(
                    sql=query.as_string(conn), con=engine, geom_col='point', parse_dates='time',
                    params={
                        't_start': context_utils.data_interval_start,
                    }
                )
                .rename(columns={'point': 'geometry'})
                .drop(['r'], axis=1)
            )

        # Convert into GeoDataFrame and transform
        gdf = (
            gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.point_x, df.point_y))
            .drop(['point_x', 'point_y'], axis=1)
        )

        # Merge the two tables
        # Then, sort the values. Otherwise, min(time) per scooter gets deleted
        # when min(time) appears after anoter time for that scooter when calling
        # pandas.DataFrame.drop_duplicates()
        logging.info(f'Size of table imported from MongoDB: {len(gdf.index)} rows.')
        logging.info(f'Size of table imported from PSQL {table_path.name}: {len(gdf_before.index)} rows.')
        gdf = (
            pd.concat([gdf_before, gdf], ignore_index=True)
            .set_geometry('geometry')
            .sort_values(['id', 'time'])
            .reset_index(drop=True)
        )

        # After UNION, delete rows, where max(time_to) per scooter ID is lower than the current execution date.
        # This means, that for this particular scooter, no path has to be recalculated again.
        # Otherwise, the time consuming path calculation will be performed again.
        gdf['_time_max_scooter'] = gdf.groupby('id')['time'].transform('max')
        gdf = gdf[pd.to_datetime(gdf['_time_max_scooter'], utc=True) > context_utils.data_interval_start]
        gdf.drop('_time_max_scooter', axis=1, inplace=True)

        # First transformation
        gdf = (gdf
            .set_crs(epsg=4326)
            .to_crs(epsg=21781)
            .drop_duplicates(subset=['id', 'time'])  # redundant info from 2 min cronjobs not needed
            .assign(_geometry_before=lambda x: x.groupby('id')['geometry'].shift())
            .assign(distance_m=lambda x: x.distance(x._geometry_before))
            .assign(_has_moved=lambda x: np.where(x['distance_m'] > POS_CHANGE_WHEN_BIGGER_THAN_METER, 1, 0))
            .assign(_has_moved_cumsum=lambda x: x.groupby('id')['_has_moved'].cumsum())
            .assign(time_from=lambda x: x.groupby(['id', '_has_moved_cumsum'])['time'].transform('first'))
            .assign(time_to=lambda x: x.groupby(['id', '_has_moved_cumsum'])['time'].transform('last'))

            # Calculate the middle point of the same position to determine a more precise location,
            # while standing still.
            .transform(
                lambda x: x.merge(
                    x.dissolve(by=['id', '_has_moved_cumsum']).geometry.centroid.rename('point_middle'),
                    how='left', left_on=['id', '_has_moved_cumsum'], right_index=True
                )
            )
            .drop_duplicates(['id', 'time_from'])
            .assign(distance_m=lambda x: x['distance_m'].replace(np.nan, 0))

            # Determine, if scooter is currently moving
            .assign(moving=lambda x: np.where((x['time_from'] == x['time_to']) & (x['_has_moved'] == 1), True, False))
            .set_geometry('point_middle', drop=True, crs='EPSG:21781')
            .drop(['time', '_geometry_before', '_has_moved', '_has_moved_cumsum'], axis=1)
        )

        # Calculate paths, on foot and on a bike.
        logging.info('Load graphs from OpenStreetMap.')
        graph_walk = ox.graph_from_bbox(
            south=WTHUR_SOUTH, north=WTHUR_NORTH,
            west=WTHUR_WEST, east=WTHUR_EAST,
            network_type='walk'
        )
        graph_bike = ox.graph_from_bbox(
            south=WTHUR_SOUTH, north=WTHUR_NORTH,
            west=WTHUR_WEST, east=WTHUR_EAST,
            network_type='bike'
        )

        # Second transformation
        logging.info('Calculate paths')
        gdf = (gdf

            # In nearest_nodes(), CRS for points and the map (graph_walk, graph_bike) mut be the same.
            .assign(_geometry_4326=lambda x: x.geometry.to_crs(epsg=4326))
            .assign(_nearest_node_walk=lambda x: ox.distance.nearest_nodes(graph_walk, x._geometry_4326.x, x._geometry_4326.y))
            .assign(_nearest_node_walk_before=lambda x: x.groupby('id')['_nearest_node_walk'].shift())
            .assign(_nearest_node_walk_before=lambda x: x['_nearest_node_walk_before'].combine_first(x['_nearest_node_walk']))
            .assign(_nearest_node_bike=lambda x: ox.distance.nearest_nodes(graph_bike, x._geometry_4326.x, x._geometry_4326.y))
            .assign(_nearest_node_bike_before=lambda x: x.groupby('id')['_nearest_node_bike'].shift())
            .assign(_nearest_node_bike_before=lambda x: x['_nearest_node_bike_before'].combine_first(x['_nearest_node_bike']))
            .assign(path_walk_since_last=lambda x:
                ox.distance.shortest_path(graph_walk, x['_nearest_node_walk_before'], x['_nearest_node_walk'], cpus=1)
            )
            .assign(path_bike_since_last=lambda x:
                ox.distance.shortest_path(graph_bike, x['_nearest_node_bike_before'], x['_nearest_node_bike'], cpus=1)
            )

            # Replace paths with only one node
            .assign(path_walk_since_last=lambda x:
                np.where(x['path_walk_since_last'].str.len() == 1, np.nan, x['path_walk_since_last'])
            )
            .assign(path_bike_since_last=lambda x:
                np.where(x['path_bike_since_last'].str.len() == 1, np.nan, x['path_bike_since_last'])
            )

            # Build shapely.LineString from nodes and change projection
            .assign(path_walk_since_last=lambda x:
                x.apply(lambda row: LineString(
                    [
                        Point(graph_walk.nodes[i]['x'], graph_walk.nodes[i]['y'])
                        for i in row['path_walk_since_last']
                    ]
                ) if isinstance(row['path_walk_since_last'], list) else np.nan, axis=1)
            )
            .assign(path_bike_since_last=lambda x:
                x.apply(lambda row: LineString(
                    [
                        Point(graph_bike.nodes[i]['x'], graph_bike.nodes[i]['y'])
                        for i in row['path_bike_since_last']
                    ]
                ) if isinstance(row['path_bike_since_last'], list) else np.nan, axis=1)
            )
            .assign(path_walk_since_last=lambda x: x['path_walk_since_last'].set_crs(epsg=4326).to_crs(epsg=21781))
            .assign(path_bike_since_last=lambda x: x['path_bike_since_last'].set_crs(epsg=4326).to_crs(epsg=21781))

            # Calculate distance on path
            .assign(distance_m_walk=lambda x: x['path_walk_since_last'].length)
            .assign(distance_m_bike=lambda x: x['path_bike_since_last'].length)
            .rename(columns={'geometry': 'point'})

            # Dop temp columns
            .drop([
                '_nearest_node_walk', '_nearest_node_walk_before', '_nearest_node_bike',
                '_nearest_node_bike_before', '_geometry_4326'
            ], axis=1)

            # Set timezone UTC for later filtering
            .assign(time_to=lambda x: pd.to_datetime(x.pop('time_to'), utc=True))
            .assign(time_from=lambda x: pd.to_datetime(x.pop('time_from'), utc=True))

            # Convert geo columns back to EPSG:4326 for insert into PSQL
            .assign(point=lambda x: x['point'].to_crs(epsg=4326).to_wkt())
            .assign(path_walk_since_last=lambda x: x['path_walk_since_last'].to_crs(epsg=4326).to_wkt())
            .assign(path_bike_since_last=lambda x: x['path_bike_since_last'].to_crs(epsg=4326).to_wkt())
        )

        # Keep only rows with the time_to in the current DAG time interval
        gdf = gdf[gdf['time_to'] >= context_utils.data_interval_start]

        # Filter out paths that did not move.
        # These are scooters who did not move for the whole DAG run time interval
        # These observations will contain empty values for columns:
        #   path_walk_since_last, path_bike_since_last, distance_m_walk, distance_m_bike
        gdf.dropna(inplace=True)

        # QA
        logging.info('Starting with QA')
        dfqa = PandasDataset(gdf)
        assert dfqa.expect_column_values_to_not_be_null('id').success
        assert dfqa.expect_column_values_to_not_be_null('provider').success
        assert dfqa.expect_column_values_to_not_be_null('point').success
        assert dfqa.expect_column_values_to_not_be_null('distance_m').success
        assert dfqa.expect_column_values_to_not_be_null('moving').success
        assert dfqa.expect_column_values_to_not_be_null('time_from').success
        assert dfqa.expect_column_values_to_not_be_null('time_to').success
        assert dfqa.expect_column_values_to_be_between('distance_m', min_value=0, max_value=20_000).success
        assert dfqa.expect_column_values_to_be_between('distance_m_walk', min_value=0, max_value=20_000).success
        assert dfqa.expect_column_values_to_be_between('distance_m_bike', min_value=0, max_value=20_000).success
        assert dfqa.expect_column_unique_value_count_to_be_between('provider', min_value=0, max_value=8).success

        # Load data into PSQL, using UPSERT
        with PostgresHook(target_conn_id).get_sqlalchemy_engine().begin() as conn:

            # Create temporary table to insert
            meta.tables[table_path_tmp.name].create(conn)

            # Insert data from one DAG run into temporary table
            rows = gdf.to_sql(table_path_tmp.name, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

            # Insert to main table (shared_mobility_ids) with UPSERT
            query = f"""
                INSERT INTO {table_path.name} as ins
                SELECT *
                FROM {table_path_tmp.name}
                ON CONFLICT (id, time_from) DO UPDATE
                SET
                    provider = EXCLUDED.provider
                    , point = EXCLUDED.point
                    , time_to = EXCLUDED.time_to
                    , distance_m = EXCLUDED.distance_m
                    , moving = EXCLUDED.moving
                    , distance_m_walk = EXCLUDED.distance_m_walk
                    , path_walk_since_last = EXCLUDED.path_walk_since_last
                    , distance_m_bike = EXCLUDED.distance_m_bike
                    , path_bike_since_last = EXCLUDED.path_bike_since_last
                ;
            """
            rows = conn.execute(query)
            logging.log(
                logging.WARNING if rows == 0 else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )


    # ################################################################################################################
    # SUBDAG: Marts
    # ################################################################################################################

    table_mart_edges = Table(
        'shared_mobility_mart_edges', meta,
        Column('id', String, primary_key=True, index=True, nullable=False),
        Column('provider', String, nullable=False),
        Column('time_from', DateTime, primary_key=True, index=True, nullable=False),
        Column('time_to', DateTime, nullable=False, index=True),
        Column('path_idx', Integer, primary_key=True, nullable=False),
        Column('point', Geometry('POINT'), nullable=False),
        Column('point_before', Geometry('POINT'), nullable=True),
        Column('distance_m', Float, nullable=True),
    )
    Index('idx_time_from_to', table_mart_edges.c.time_from, table_mart_edges.c.time_to)

    @task(task_id='mart_edges', retries=1, retry_delay=dt.timedelta(minutes=5))
    def mart_edges(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        query = sql.SQL("""
            INSERT INTO {mart}
            SELECT
                t2.*
                , ST_DISTANCE(
                    ST_Transform(ST_SetSRID(point_before, 4326), 3857),
                    ST_Transform(ST_SetSRID(point, 4326), 3857)
                ) as distance_m
            FROM (
                SELECT
                    id
                    , provider
                    , time_from
                    , time_to
                    , coalesce(path[1], 0) AS path_idx
                    , geom AS point
                    , LAG(geom) OVER (PARTITION BY t.id ORDER BY t.time_to, path) AS point_before
                FROM (
                    SELECT
                        id, provider, time_from, time_to,
                        (ST_DumpPoints(path_walk_since_last)).*
                    FROM {source}
                    WHERE
                        time_from BETWEEN %(t_start)s AND %(t_end)s
                ) t
            )t2
            ON CONFLICT (id, time_from, path_idx) DO UPDATE
            SET
                provider = EXCLUDED.provider
                , time_to = EXCLUDED.time_to
                , point = EXCLUDED.point
                , point_before = EXCLUDED.point_before
                , distance_m = EXCLUDED.distance_m;
        """).format(
            source=sql.Identifier(table_path.name),
            mart=sql.Identifier(table_mart_edges.name),
        )
        conn = PostgresHook(target_conn_id).get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, dict(t_start=context_utils.data_interval_start, t_end=context_utils.data_interval_end))
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )
            conn.commit()
        finally:
            conn.close()

    table_mart_distinct_ids = Table(
        'shared_mobility_mart_distinct_ids', meta,
        Column('provider', String, primary_key=True, nullable=False),
        Column('time', DateTime, primary_key=True, index=True, nullable=False),
        Column('distinct_ids', Integer, nullable=False),
    )

    @task(task_id='mart_distinct_ids', retries=1, retry_delay=dt.timedelta(minutes=5))
    def mart_distinct_ids(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        query = sql.SQL("""
            INSERT INTO {mart}
            SELECT
                provider
                , date_trunc('day', time_from) as time
                , COUNT(DISTINCT id) distinct_ids
            FROM {source}
            WHERE
                date_trunc('day', time_from) = %(t_start)s::date
            GROUP BY provider, time
            ON CONFLICT (provider, time) DO UPDATE
            SET
                distinct_ids = EXCLUDED.distinct_ids;
        """).format(
            source=sql.Identifier(table_path.name),
            mart=sql.Identifier(table_mart_distinct_ids.name),
        )
        conn = PostgresHook(target_conn_id).get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, dict(t_start=context_utils.data_interval_start))
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {table_mart_distinct_ids.name}: {rows} rows were affected.'
            )
            conn.commit()
        finally:
            conn.close()

    table_mart_trip_distance = Table(
        'shared_mobility_mart_trip_distance', meta,
        Column('id', String, primary_key=True, index=True, nullable=False),
        Column('provider', String, nullable=False),
        Column('trip_id', String, primary_key=True, nullable=False),
        Column('trip_start', DateTime, nullable=False),
        Column('trip_end', DateTime, nullable=False),
        Column('trip_walk_distance_m', Float, nullable=True),
        Column('trip_bike_distance_m', Float, nullable=True),
    )

    @task(task_id='mart_trip_distance', retries=1, retry_delay=dt.timedelta(minutes=5))
    def mart_trip_distance(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        query = sql.SQL("""
            INSERT INTO {mart}
            select
                id
                , provider
                , trip_id
                , min(time_from) as trip_start
                , max(time_from) as trip_end
                , sum(distance_m_walk) as trip_walk_distance_m
                , sum(distance_m_bike) as trip_bike_distance_m
            FROM (
                SELECT
                    t.*
                    , SUM(_count_up) OVER (PARTITION BY id ORDER BY time_from) trip_id
                FROM (
                    SELECT
                        id
                        , provider
                        , time_from
                        , time_to
                        , moving
                        , CASE WHEN moving then 0 else 1 end as _count_up
                        , distance_m_walk
                        , distance_m_bike
                    FROM {source}
                ) t
            ) t2
            WHERE
                t2.time_from BETWEEN %(t_start)s AND %(t_end)s
            group by provider, id, trip_id
            ON CONFLICT (id, trip_id) DO UPDATE
            SET
                trip_start = EXCLUDED.trip_start
                , trip_end = EXCLUDED.trip_end
                , trip_walk_distance_m = EXCLUDED.trip_walk_distance_m
                , trip_bike_distance_m = EXCLUDED.trip_bike_distance_m;
        """).format(
            source=sql.Identifier(table_path.name),
            mart=sql.Identifier(table_mart_trip_distance.name),
        )
        conn = PostgresHook(target_conn_id).get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, dict(t_start=context_utils.data_interval_start, t_end=context_utils.data_interval_end))
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {table_mart_trip_distance.name}: {rows} rows were affected.'
            )
            conn.commit()
        finally:
            conn.close()

    table_mart_scooter_age = Table(
        'shared_mobility_mart_scooter_age', meta,
        Column('time', DateTime, primary_key=True, nullable=False),
        Column('provider', String, primary_key=True, nullable=False),
        Column('avg_age_days_scooter', Float, nullable=False),
    )

    @task(task_id='mart_scooter_age', retries=1, retry_delay=dt.timedelta(minutes=5))
    def mart_scooter_age(target_conn_id: str):
        context_utils = AirflowContextUtils(get_current_context())
        query = sql.SQL("""
            INSERT INTO {mart}
            SELECT
                time
                , provider
                , AVG(current_age_days) as avg_age_days_scooter
            from (
                select
                    date_trunc('hour', day) as time
                    , EXTRACT(EPOCH FROM (date_trunc('hour', day) - t.min_time)) / 86400 as current_age_days
                    , provider
                from (
                    select
                        id
                        , provider
                        , min(time_from) min_time
                        , max(time_to) max_time
                    FROM {source}
                    group by id, provider
                ) t
                CROSS JOIN generate_series(t.min_time + interval '1' hour, t.max_time, interval '1 hour') as g(day)
                WHERE
                    date_trunc('hour', day) between t.min_time and t.max_time
            ) t2
            WHERE
                time BETWEEN %(t_start)s AND %(t_end)s
            GROUP BY time, provider
            ORDER BY time
        """).format(
            source=sql.Identifier(table_path.name),
            mart=sql.Identifier(table_mart_scooter_age.name),
        )
        conn = PostgresHook(target_conn_id).get_conn()
        try:
            cur = conn.cursor()
            cur.execute(query, dict(t_start=context_utils.data_interval_start, t_end=context_utils.data_interval_end))
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {table_mart_scooter_age.name}: {rows} rows were affected.'
            )
            conn.commit()
        finally:
            conn.close()


    # TODO: Maybe add GreatExpectation tests for data marts

    # ################################################################################################################
    # TASK CONFIGURATION
    # ################################################################################################################

    # wait_for_downstream prevents execution of current DAG if last DAG run was not successful
    # See: https://airflow.apache.org/docs/apache-airflow/stable/faq.html#why-isn-t-my-task-getting-scheduled
    t_begin_assert_table = DummyOperator(
        task_id='begin_assert_table',
        depends_on_past=True,
    )

    # PATH
    t_assert_path = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_path.name,
        task_id='path_assert_table'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_path.set_upstream(t_begin_assert_table)
    t_etl_path = path_etl.expand(target_conn_id=CONFIG.conn_ids)
    t_etl_path.set_upstream(t_assert_path)
    t_delete_old_path = (
        DeleteOldRows
        .partial(task_id='delete_old_path', config=CONFIG, table_name=table_path.name, column_name=table_path.c.time_to.name)
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_path.set_upstream(t_etl_path)

    # PROVIDER
    t_assert_provider = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_provider.name,
        task_id='provider_assert_table'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_provider.set_upstream(t_begin_assert_table)
    t_etl_provider = provider_etl.expand(target_conn_id=CONFIG.conn_ids)
    t_etl_provider.set_upstream(t_assert_provider)
    t_delete_old_provider = (
        DeleteOldRows
        .partial(task_id='delete_old_provider', config=CONFIG, table_name=table_provider.name, column_name=table_provider.c.time.name)
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_provider.set_upstream(t_etl_provider)

    # ID
    t_assert_id = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_id.name,
        task_id='id_assert_table'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_id.set_upstream(t_begin_assert_table)
    t_etl_id = id_etl.expand(target_conn_id=CONFIG.conn_ids)
    t_etl_id.set_upstream(t_assert_id)
    t_delete_old_id = (
        DeleteOldRows
        .partial(task_id='delete_old_id', config=CONFIG, table_name=table_id.name, column_name=table_id.c.last_seen.name)
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_id.set_upstream(t_etl_id)

    t_end_assert_table = DummyOperator(task_id='end_assert_table', trigger_rule='none_failed')
    t_end_assert_table.set_upstream([t_delete_old_path, t_delete_old_provider, t_delete_old_id])

    t_count_rows = (
        AssertPathRowsExecutionDate
        .partial(task_id='assert_rows_in_path')
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_count_rows.set_upstream(t_end_assert_table)

    t_begin_calculate_marts = DummyOperator(task_id='begin_calculate_marts')
    t_begin_calculate_marts.set_upstream(t_count_rows)

    t_assert_table_mart_edges = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_mart_edges.name,
        task_id='assert_table_mart_edges'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_table_mart_edges.set_upstream(t_begin_calculate_marts)
    t_mart_edges = mart_edges.expand(target_conn_id=CONFIG.conn_ids)
    t_mart_edges.set_upstream(t_assert_table_mart_edges)
    t_delete_old_mart_edges = (
        DeleteOldRows
        .partial(
            task_id='delete_old_mart_edges', config=CONFIG,
            table_name=table_mart_edges.name, column_name=table_mart_edges.c.time_to.name,
        )
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_mart_edges.set_upstream(t_mart_edges)

    t_assert_table_mart_distinct_ids = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_mart_distinct_ids.name,
        task_id='assert_table_mart_distinct_ids'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_table_mart_distinct_ids.set_upstream(t_begin_calculate_marts)
    t_mart_distinct_ids = mart_distinct_ids.expand(target_conn_id=CONFIG.conn_ids)
    t_mart_distinct_ids.set_upstream(t_assert_table_mart_distinct_ids)
    t_delete_old_mart_distinct_ids = (
        DeleteOldRows
        .partial(
            task_id='delete_old_mart_distinct_ids', config=CONFIG,
            table_name=table_mart_distinct_ids.name, column_name=table_mart_distinct_ids.c.time.name,
        )
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_mart_distinct_ids.set_upstream(t_mart_distinct_ids)

    t_assert_table_mart_trip_distance = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_mart_trip_distance.name,
        task_id='assert_table_mart_trip_distance'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_table_mart_trip_distance.set_upstream(t_begin_calculate_marts)
    t_mart_trip_distance = mart_trip_distance.expand(target_conn_id=CONFIG.conn_ids)
    t_mart_trip_distance.set_upstream(t_assert_table_mart_trip_distance)
    t_delete_old_mart_trip_distance = (
        DeleteOldRows
        .partial(
            task_id='delete_old_mart_trip_distance', config=CONFIG,
            table_name=table_mart_trip_distance.name, column_name=table_mart_trip_distance.c.trip_end.name,
        )
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_mart_trip_distance.set_upstream(t_mart_trip_distance)

    t_assert_table_mart_scooter_age = CheckOrCreatePostgresTableOperator.partial(
        meta=meta, table_name=table_mart_scooter_age.name,
        task_id='assert_table_mart_scooter_age'
    ).expand(target_conn_id=CONFIG.conn_ids)
    t_assert_table_mart_scooter_age.set_upstream(t_begin_calculate_marts)
    t_mart_scooter_age = mart_scooter_age.expand(target_conn_id=CONFIG.conn_ids)
    t_mart_scooter_age.set_upstream(t_assert_table_mart_scooter_age)
    t_delete_old_mart_scooter_age = (
        DeleteOldRows
        .partial(
            task_id='delete_old_mart_scooter_age', config=CONFIG,
            table_name=table_mart_scooter_age.name, column_name=table_mart_scooter_age.c.time.name,
        )
        .expand(target_conn_id=CONFIG.conn_ids)
    )
    t_delete_old_mart_scooter_age.set_upstream(t_mart_scooter_age)

