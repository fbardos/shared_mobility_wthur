"""

DAG to calculate paths of scooters.

    1. Assert, target table is available
    2. Insert rows. Each row represents a position and the duration, how long the scooter stayed there.
        2.1 Get data from one day from MongoDB
        2.2 Load last known position from PSQL
        2.3 If position has not changed since last record, update the existing row in PSQL (time wise)
    3. Calculate the distance driven from the last point (for walking and bycicle)

Rows:
    - Scooter id
    - Geopoint
    - time_arrive
    - time_depart
    - duration_resting
    - moving (True, when arrive and depart are the same)
    ------- Updated later:
    - distance_to_last_foot
    - subpath_to_last_foot (contains the edges from openstreetmap for the path)
    - distance_to_last_bicycle
    - subpath_to_last_bicycle (contains the edges from openstreetmap for the path)

"""
import datetime as dt
import logging
from typing import Any

import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
from dateutil import tz
from geoalchemy2 import Geometry
from great_expectations.dataset import PandasDataset
from psycopg2 import sql
from shapely.geometry import LineString, Point
from sqlalchemy import (Boolean, Column, DateTime, Float, Index, Integer,
                        String, delete)
from sqlalchemy.orm import declarative_base

from airflow import DAG
from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import get_current_context
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id='shared_mobility_wthur',
    schedule_interval='5 2 * * *',  # every day at 02:00
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
    POS_CHANGE_WHEN_BIGGER_THAN_METER = 50
    PSQL_CONN_ID = 'psql_marts'
    PSQL_PUBLIC_CONN_ID = 'psql_public'
    MONGO_CONN_ID = 'mongo_opendata'

    # SQLAlchemy ORM: table definitions
    Base = declarative_base()

    class CheckOrCreatePostgresTableOperator(BaseOperator):

        def __init__(self, declarative_base: Any, table_name: str, target_conn_id: str, **kwargs):
            super().__init__(**kwargs)
            self._declarative_base = declarative_base
            self._table_name = table_name
            self._target_conn_id = target_conn_id

        # def execute(self, context, target_conn_id: str):
        def execute(self, context):
            hook = PostgresHook(self._target_conn_id)
            engine = hook.get_sqlalchemy_engine()

            # If table is not existent, create table
            with engine.connect() as conn:
                if not engine.dialect.has_table(conn, self._table_name):
                    self._declarative_base.metadata.tables[self._table_name].create(engine)

    class Path(Base):
        __tablename__ = 'shared_mobility_path'

        id = Column(String, primary_key=True, nullable=False)
        provider = Column(String, nullable=False)
        point = Column(Geometry('POINT'), nullable=False)
        time_from = Column(DateTime, primary_key=True, nullable=False)
        time_to = Column(DateTime, nullable=False)
        distance_m = Column(Float, nullable=False)
        moving = Column(Boolean, nullable=False)
        distance_m_walk = Column(Float, nullable=True)
        path_walk_since_last = Column(Geometry('LINESTRING'), nullable=True)
        distance_m_bike = Column(Float, nullable=True)
        path_bike_since_last = Column(Geometry('LINESTRING'), nullable=True)

    class Provider(Base):
        __tablename__ = 'shared_mobility_provider'

        time = Column(DateTime, primary_key=True, nullable=False)
        provider = Column(String, primary_key=True, nullable=False)
        count_datapoints = Column(Integer, nullable=False)
        count_distinct_ids = Column(Integer, nullable=False)
        avg_delta_updated = Column(Float, nullable=False)
        count_available = Column(Integer, nullable=False)
        count_disabled = Column(Integer, nullable=False)
        count_reserved = Column(Integer, nullable=False)

    # ################################################################################################################
    # SUBDAG: Provider
    # ################################################################################################################

    # Task: Load from Mongo, build mart and write to Postgres
    @task(task_id='provider_etl')
    def provider_etl(target_conn_id: str):
        airflow_context = get_current_context()
        execution_date = airflow_context.get('execution_date')
        assert isinstance(execution_date, dt.datetime)

        mongo_hook = MongoHook(MONGO_CONN_ID)
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
        psql_hook = PostgresHook(target_conn_id)
        engine = psql_hook.get_sqlalchemy_engine()
        df.to_sql(Provider.__tablename__, engine, index=False, if_exists='append')

    # ################################################################################################################
    # SUBDAG: PATH
    # ################################################################################################################

    # Task: Load from Mongo, build mart and write to Postgres
    @task(task_id='path_etl')
    def path_etl(target_conn_id: str):
        airflow_context = get_current_context()
        execution_date = airflow_context.get('execution_date')
        assert isinstance(execution_date, dt.datetime)

        # Create PSQL engine
        psql_hook = PostgresHook(target_conn_id)
        engine = psql_hook.get_sqlalchemy_engine()


        # Load all datapoints for the given day
        logging.info('Load data from MongoDB')
        mongo_hook = MongoHook(MONGO_CONN_ID)
        with mongo_hook.get_conn() as client:
            start = dt.datetime(execution_date.year, execution_date.month, execution_date.day, tzinfo=tz.tzutc())
            end = dt.datetime(execution_date.year, execution_date.month, execution_date.day, 23, 59, 59, tzinfo=tz.tzutc())
            query = {
                'geometry.coordinates.1': {'$gt': WTHUR_SOUTH, '$lt': WTHUR_NORTH},
                'geometry.coordinates.0': {'$gt': WTHUR_WEST, '$lt': WTHUR_EAST},
                '_meta_runtime_utc': {'$gt': start, '$lt': end},
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
            # .assign(id=lambda x: x['properties'].apply(lambda y: y['id']))
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
                    ROW_NUMBER() OVER (PARTITION BY id, time_to ORDER BY time_to desc) as r,
                    mart.id,
                    mart.provider,
                    mart.time_to,
                    mart.point
                from {mart} mart
                where time_to < %(execution_date)s
            ) x
            where
                x.r <= 2
        """).format(mart=sql.Identifier(Path.__tablename__))

        with psql_hook.get_conn() as conn:
            gdf_before = (
                gpd.GeoDataFrame.from_postgis(
                    sql=query.as_string(conn), con=engine, geom_col='point', parse_dates='time_to',
                    params={
                        'execution_date': execution_date,
                    }
                )
                .rename(columns={'time_to': 'time', 'point': 'geometry'})
                .drop(['r'], axis=1)
            )

        # Convert into GeoDataFrame and transform
        gdf = (
            gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.point_x, df.point_y))
            .drop(['point_x', 'point_y'], axis=1)
        )

        # Merge the two tables
        gdf = pd.concat([gdf_before, gdf], ignore_index=True).set_geometry('geometry')

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
            .drop(['time', '_geometry_before', '_has_moved', '_has_moved_cumsum'], axis=1)

            # Determine, if scooter is currently moving
            .assign(moving=lambda x: np.where(x['time_from'] == x['time_to'], True, False))
            .set_geometry('point_middle', drop=True, crs='EPSG:21781')
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
                ox.distance.shortest_path(graph_walk, x['_nearest_node_walk_before'], x['_nearest_node_walk'], cpus=None)
            )
            .assign(path_bike_since_last=lambda x:
                ox.distance.shortest_path(graph_bike, x['_nearest_node_bike_before'], x['_nearest_node_bike'], cpus=None)
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

        # Keep only rows with the time_to in the current DAG execution date
        # Also filters scooter IDs who are not in use anymore. These could be filtered earlier on.
        gdf = gdf[gdf['time_to'] >= execution_date]

        # QA
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

        # Iterate over all rows. When time_from is before DAG execution date, then delete the
        # corresponding row in PSQL before inserting the updated one later.
        with engine.begin() as conn:
            t = pd.to_datetime(execution_date)
            for _, row in gdf[(gdf['time_from'] < t) & (gdf['time_to'] > t)][['id', 'time_from']].iterrows():
                stmt = (
                    delete(Path)
                    .where(Path.id == row['id'])
                    .where(Path.time_from == row['time_from'])
                )
                conn.execute(stmt)
                logging.info(f"Deleted for later upsert: {stmt}")

        # Load to PSQL
        # Maybe solve problem with old row with this: https://stackoverflow.com/a/63189754/4856719
        gdf.to_sql(Path.__tablename__, engine, index=False, if_exists='append')

    # ################################################################################################################
    # SUBDAG: Marts
    # ################################################################################################################

    class MartEdges(Base):
        __tablename__ = 'shared_mobility_mart_edges'

        id = Column(String, primary_key=True, index=True, nullable=False)
        provider = Column(String, nullable=False)
        time_from = Column(DateTime, primary_key=True, index=True, nullable=False)
        time_to = Column(DateTime, nullable=False, index=True)
        path_idx = Column(Integer, primary_key=True, nullable=False)
        point = Column(Geometry('POINT'), nullable=False)
        point_before = Column(Geometry('POINT'), nullable=True)
        distance_m = Column(Float, nullable=True)

        __table_args__ = (
            Index('idx_time_from_to', 'time_from', 'time_to'),
        )

    @task(task_id='mart_edges')
    def mart_edges(target_conn_id: str):
        airflow_context = get_current_context()
        execution_date = airflow_context.get('execution_date')
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
                        time_to::date = %(execution_date)s
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
            source=sql.Identifier(Path.__tablename__),
            mart=sql.Identifier(MartEdges.__tablename__),
        )
        with PostgresHook(target_conn_id).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                query,
                {
                    'execution_date': execution_date,
                }
            )
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {MartEdges.__tablename__}: {rows} rows were affected.'
            )
            conn.commit()


    class MartDistinctIds(Base):
        __tablename__ = 'shared_mobility_mart_distinct_ids'

        provider = Column(String, primary_key=True, nullable=False)
        time = Column(DateTime, primary_key=True, index=True, nullable=False)
        distinct_ids = Column(Integer, nullable=False)

    @task(task_id='mart_distinct_ids')
    def mart_distinct_ids(target_conn_id: str):
        airflow_context = get_current_context()
        execution_date = airflow_context.get('execution_date')

        query = sql.SQL("""
            INSERT INTO {mart}
            SELECT
                provider
                , date_trunc('day', time_from) as time
	            , COUNT(DISTINCT id) distinct_ids
            FROM {source}
            WHERE
                time_to::date = %(execution_date)s
            GROUP BY provider, time
            ON CONFLICT (provider, time) DO UPDATE
            SET
                distinct_ids = EXCLUDED.distinct_ids;
        """).format(
            source=sql.Identifier(Path.__tablename__),
            mart=sql.Identifier(MartDistinctIds.__tablename__),
        )
        with PostgresHook(target_conn_id).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                query,
                {
                    'execution_date': execution_date,
                }
            )
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {MartDistinctIds.__tablename__}: {rows} rows were affected.'
            )
            conn.commit()


    class MartTripDistance(Base):
        __tablename__ = 'shared_mobility_mart_trip_distance'

        id = Column(String, primary_key=True, index=True, nullable=False)
        provider = Column(String, nullable=False)
        trip_id = Column(String, primary_key=True, nullable=False)
        trip_start = Column(DateTime, nullable=False)
        trip_end = Column(DateTime, nullable=False)
        trip_walk_distance_m = Column(Float, nullable=True)

    @task(task_id='mart_trip_distance')
    def mart_trip_distance(target_conn_id: str):
        query = sql.SQL("""
            INSERT INTO {mart}
            select
                id
                , provider
                , trip_id
                , min(time_from) as trip_start
                , max(time_to) as trip_end
                , sum(distance_m_walk) as trip_walk_distance_m
            FROM (
                SELECT
                    t.*
                    , SUM(_count_up) OVER (PARTITION BY id ORDER BY time_to) trip_id
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
            group by provider, id, trip_id
            ON CONFLICT (id, trip_id) DO UPDATE
            SET
                trip_start = EXCLUDED.trip_start
                , trip_end = EXCLUDED.trip_end
                , trip_walk_distance_m = EXCLUDED.trip_walk_distance_m;
        """).format(
            source=sql.Identifier(Path.__tablename__),
            mart=sql.Identifier(MartTripDistance.__tablename__),
        )
        with PostgresHook(target_conn_id).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()


    class MartScooterAge(Base):
        __tablename__ = 'shared_mobility_mart_scooter_age'

        time = Column(DateTime, primary_key=True, nullable=False)
        provider = Column(String, primary_key=True, nullable=False)
        avg_age_days_scooter = Column(Float, nullable=False)

    @task(task_id='mart_scooter_age')
    def mart_scooter_age(target_conn_id: str):
        airflow_context = get_current_context()

        # TODO: Not DRY
        execution_date = airflow_context.get('execution_date')
        start = dt.datetime(execution_date.year, execution_date.month, execution_date.day, tzinfo=tz.tzutc())
        end = dt.datetime(execution_date.year, execution_date.month, execution_date.day, 23, 59, 59, tzinfo=tz.tzutc())

        query = sql.SQL("""
            INSERT INTO {mart}
            SELECT
                time
                , provider
                , AVG(current_age_days) as avg_age_days_scooter
            from (
                select
                    date_trunc('hour', day) as time
                    -- , EXTRACT(EPOCH FROM CASE
                    --     WHEN date_trunc('hour', day) > t.max_time THEN t.max_time - t.min_time
                    --     ELSE date_trunc('hour', day) - t.min_time
                    --   END) / 86400 as current_age_days
                    , EXTRACT(EPOCH FROM (date_trunc('hour', day) - t.min_time)) / 86400 as current_age_days
                    , provider
                from (
                    select
                        id
                        , provider
                        , min(time_from) min_time
                        , max(time_to) max_time
                    FROM {source}
                    -- This has to be disabled. Otherwise it will not get the whole range of scooters IDs.
                    -- WHERE
                    --     time_to >= %(start)s
                    --     and time_from <= %(end)s
                    group by id, provider
                ) t
                CROSS JOIN generate_series(t.min_time + interval '1' hour, t.max_time, interval '1 hour') as g(day)
                WHERE
                    date_trunc('hour', day) between t.min_time and t.max_time
            ) t2
            WHERE
                time::date = %(execution_date)s
            GROUP BY time, provider
            ORDER BY time
        """).format(
            source=sql.Identifier(Path.__tablename__),
            mart=sql.Identifier(MartScooterAge.__tablename__),
        )
        with PostgresHook(target_conn_id).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                query,
                {
                    'start': start,
                    'end': end,
                    'execution_date': execution_date,
                }
            )
            logging.log(
                logging.WARNING if (rows := cur.rowcount) == 0 else logging.INFO,
                f'Table {MartScooterAge.__tablename__}: {rows} rows were affected.'
            )
            conn.commit()


    # TODO: Maybe add GreatExpectation tests for data marts

    # ################################################################################################################
    # TASK CONFIGURATION
    # ################################################################################################################

    # wait_for_downstream prevents execution of current DAG if last DAG run was not successful
    # See: https://airflow.apache.org/docs/apache-airflow/stable/faq.html#why-isn-t-my-task-getting-scheduled
    t_begin_assert_table = DummyOperator(
        task_id='begin_assert_table',
        depends_on_past=True,
        wait_for_downstream=True,
    )

    DUPLICATE_CONN_IDS = (PSQL_CONN_ID, PSQL_PUBLIC_CONN_ID)

    t_assert_path = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=Path.__tablename__,
        task_id='path_assert_table'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_path.set_upstream(t_begin_assert_table)
    t_etl_path = path_etl.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_etl_path.set_upstream(t_assert_path)

    t_assert_provider = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=Provider.__tablename__,
        task_id='provider_assert_table'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_provider.set_upstream(t_begin_assert_table)
    t_etl_provider = provider_etl.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_etl_provider.set_upstream(t_assert_provider)

    t_end_assert_table = DummyOperator(task_id='end_assert_table')
    t_end_assert_table.set_upstream([t_etl_path, t_etl_provider])

    t_begin_calculate_marts = DummyOperator(task_id='begin_calculate_marts')
    t_begin_calculate_marts.set_upstream(t_end_assert_table)

    t_assert_table_mart_edges = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=MartEdges.__tablename__,
        task_id='assert_table_mart_edges'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_table_mart_edges.set_upstream(t_begin_calculate_marts)
    t_mart_edges = mart_edges.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_mart_edges.set_upstream(t_assert_table_mart_edges)

    t_assert_table_mart_distinct_ids = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=MartDistinctIds.__tablename__,
        task_id='assert_table_mart_distinct_ids'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_table_mart_distinct_ids.set_upstream(t_begin_calculate_marts)
    t_mart_distinct_ids = mart_distinct_ids.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_mart_distinct_ids.set_upstream(t_assert_table_mart_distinct_ids)

    t_assert_table_mart_trip_distance = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=MartTripDistance.__tablename__,
        task_id='assert_table_mart_trip_distance'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_table_mart_trip_distance.set_upstream(t_begin_calculate_marts)
    t_mart_trip_distance = mart_trip_distance.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_mart_trip_distance.set_upstream(t_assert_table_mart_trip_distance)

    t_assert_table_mart_scooter_age = CheckOrCreatePostgresTableOperator.partial(
        declarative_base=Base, table_name=MartScooterAge.__tablename__,
        task_id='assert_table_mart_scooter_age'
    ).expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_assert_table_mart_scooter_age.set_upstream(t_begin_calculate_marts)
    t_mart_scooter_age = mart_scooter_age.expand(target_conn_id=DUPLICATE_CONN_IDS)
    t_mart_scooter_age.set_upstream(t_assert_table_mart_scooter_age)

