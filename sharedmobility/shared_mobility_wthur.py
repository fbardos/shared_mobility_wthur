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
from geoalchemy2 import Geometry
from great_expectations.dataset import PandasDataset
from psycopg2 import sql
from shapely.geometry import LineString, Point
from sqlalchemy import (Boolean, Column, DateTime, Float, Index, Integer,
                        MetaData, String, Table)
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
from pyproj import CRS

# Cannot use sqlalchemy.orm.declarative_base() in the airflow context.
# Otherwise, the same MetaData object as for the metadatabase of airflow is used.
# This creates many conflicts during DagBag filling.
meta = MetaData()

# TODO: MOVE all talbe definitions to this position, and change to uppercase
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
TABLE_PATH = Table(
    'shared_mobility_path', meta,
    *(i.copy() for i in table_path_cols)
)
TABLE_PATH_TMP = Table(
    'shared_mobility_path_tmp', meta,
    *(i.copy() for i in table_path_cols),
    prefixes=['TEMPORARY']
)

TABLE_PROVIDER = Table(
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
TABLE_ID = Table(
    'shared_mobility_ids', meta,
    *(i.copy() for i in table_id_cols)
)
TABLE_ID_TMP = Table(
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


@dataclass(frozen=True)
class SharedMobilityConfig:
    pos_south: float = 47.449753
    pos_north: float = 47.532571
    pos_west: float = 8.687158
    pos_east: float = 8.784503
    pos_change_when_bigger_than_meter: int = 50
    mongo_conn_id: str = 'mongo_opendata'
    config: SharedMobilityConnectionStrings = SharedMobilityConnectionStrings(
        conn_id_private='psql_marts',
        conn_id_public='psql_public',
        keep_public_days=365,
    )


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


# ################################################################################################################
# ETL Operators
# Better to perform unit tests later.
# ################################################################################################################

class SharedMobilityOperator(BaseOperator):

    @staticmethod
    def build_context_utils(context: Context):
        return AirflowContextUtils(context)

    def get_data_interval_start(self, context: Context) -> dt.datetime:
        time = context.get('data_interval_start')
        assert isinstance(time, dt.datetime)
        return time

    def get_data_interval_end(self, context: Context) -> dt.datetime:
        time = context.get('data_interval_end')
        assert isinstance(time, dt.datetime)
        return time - dt.timedelta(microseconds=1)


class SharedMobilityPathEtlOperator(SharedMobilityOperator):

    def __init__(self, meta: MetaData, config: SharedMobilityConfig, target_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self._meta = meta
        self._config = config
        self._target_conn_id = target_conn_id
        self._psql_hook = PostgresHook(self._target_conn_id)  # This could make DAG loading slow

    def _load_data_from_mongo(self, context: Context, config: SharedMobilityConfig) -> pd.DataFrame:
        """Load all datapoints for the given day."""
        logging.info('Load data from MongoDB')
        mongo_hook = MongoHook(dag_config.mongo_conn_id)
        with mongo_hook.get_conn() as client:
            query = {
                'geometry.coordinates.1': {'$gt': config.pos_south, '$lt': config.pos_north},
                'geometry.coordinates.0': {'$gt': config.pos_west, '$lt': config.pos_east},
                # Datetime is stored as UTC
                '_meta_runtime_utc': {
                    '$gt': self.get_data_interval_start(context),
                    '$lt': self.get_data_interval_end(context)
                },
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
            return pd.DataFrame(list(cursor))

    def _extract_data_from_mongodb_df(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from MongoDB result set."""
        logging.info('Transform datapoints')
        df = (data
            .assign(provider=lambda x: x['properties'].apply(lambda y: y['provider']['name']))
            .assign(time=lambda x: x['_meta_last_updated_utc'])
            .assign(point_x=lambda x: x['geometry'].apply(lambda y: y['coordinates'][0]))
            .assign(point_y=lambda x: x['geometry'].apply(lambda y: y['coordinates'][1]))
            .sort_values(by=['id', 'time'])
            [['id', 'provider', 'time', 'point_x', 'point_y']]
        )

        # Convert into GeoDataFrame and transform
        df = (
            gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.point_x, df.point_y))
            .drop(['point_x', 'point_y'], axis=1)
        )
        return df

    def _load_data_from_past_locations(self, context: Context) -> gpd.GeoDataFrame:
        """
        Load the last 2 rows (t-1, t-2) of every group id and time_to to compare later with
        the first entry of the DAG execution date (t0).
        When position t-1 == t0, then the second last point (t-2) is needed to calculate
        the correct path.
        """
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
        """).format(mart=sql.Identifier(TABLE_PATH.name))
        with self._psql_hook.get_conn() as conn:
            gdf_before = (
                gpd.GeoDataFrame.from_postgis(
                    sql=query.as_string(conn),
                    con=self._psql_hook.get_sqlalchemy_engine(),
                    geom_col='point', parse_dates='time',
                    params={
                        't_start': self.get_data_interval_start(context),
                    }
                )
                .rename(columns={'point': 'geometry'})
                .drop(['r'], axis=1)
            )
        return gdf_before

    def _merge_old_and_new_data(self, old: gpd.GeoDataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """
        Merge the two tables
        Then, sort the values. Otherwise, min(time) per scooter gets deleted
        when min(time) appears after anoter time for that scooter when calling
        pandas.DataFrame.drop_duplicates()
        """
        logging.info(f'Size of table imported from MongoDB: {len(new.index)} rows.')
        logging.info(f'Size of table imported from PSQL {TABLE_PATH.name}: {len(old.index)} rows.')
        df = (
            pd.concat([old, new], ignore_index=True)
            .set_geometry('geometry')
            .sort_values(['id', 'time'])
            .reset_index(drop=True)
        )
        return df

    def _get_graph_from_bbox(self, graph_type: str):
         return ox.graph_from_bbox(
            south=self._config.pos_south, north=self._config.pos_north,
            west=self._config.pos_west, east=self._config.pos_east,
            network_type=graph_type
        )

    @property
    def graph_walk(self):
        return self._get_graph_from_bbox('walk')

    @property
    def graph_bike(self):
        return self._get_graph_from_bbox('bike')

    def _transform_and_calculate_paths(self, data: pd.DataFrame, context: Context) -> pd.DataFrame:
        gdf = data

        # After UNION, delete rows, where max(time_to) per scooter ID is lower than the current execution date.
        # This means, that for this particular scooter, no path has to be recalculated again.
        # Otherwise, the time consuming path calculation will be performed again.
        gdf['_time_max_scooter'] = gdf.groupby('id')['time'].transform('max')
        gdf = gdf[pd.to_datetime(gdf['_time_max_scooter'], utc=True) > self.get_data_interval_start(context)]
        gdf.drop('_time_max_scooter', axis=1, inplace=True)

        # First transformation
        gdf = (gdf
            .set_crs(epsg=4326)
            .to_crs(epsg=21781)
            .drop_duplicates(subset=['id', 'time'])  # redundant info from 2 min cronjobs not needed
            .assign(_geometry_before=lambda x: x.groupby('id')['geometry'].shift())
            .assign(distance_m=lambda x: x.distance(x._geometry_before))
            .assign(_has_moved=lambda x: np.where(x['distance_m'] > self._config.pos_change_when_bigger_than_meter, 1, 0))
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
        graph_walk = self.graph_walk
        graph_bike = self.graph_bike

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
            
            # Currently, path_walk/bike_since_last are not initialized as geometry and will not return as 
            # Geopandas.GeoSeries when column is called. Without set_geometry, would return pandas.Series,
            # Which leads to errors when calling Geopandas.GeoSeries.set_crs()
            .set_geometry('path_walk_since_last', crs=CRS.from_epsg(4326))
            .assign(path_walk_since_last=lambda x: x['path_walk_since_last'].to_crs(epsg=21781))
            .set_geometry('path_bike_since_last', crs=CRS.from_epsg(4326))
            .assign(path_bike_since_last=lambda x: x['path_bike_since_last'].to_crs(epsg=21781))

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

            # Convert geo columns back to EPSG:4326, then to strings for insert into PSQL
            # After this step, there are no geometry columns left. The GeoDataFrame gets
            # reshaped as DataFrame with geopandas >= 0.11.1
            .assign(point=lambda x: x['point'].to_crs(epsg=4326).to_wkt())
            .assign(path_walk_since_last=lambda x: x['path_walk_since_last'].to_crs(epsg=4326).to_wkt())
            .assign(path_bike_since_last=lambda x: x['path_bike_since_last'].to_crs(epsg=4326).to_wkt())
        )

        # Keep only rows with the time_to in the current DAG time interval
        gdf = gdf[gdf['time_to'] >= self.get_data_interval_start(context)]

        # Filter out paths that did not move.
        # These are scooters who did not move for the whole DAG run time interval
        # These observations will contain empty values for columns:
        #   path_walk_since_last, path_bike_since_last, distance_m_walk, distance_m_bike
        gdf.dropna(inplace=True)

        return gdf

    def _perform_data_qa(self, data: pd.DataFrame) -> None:
        logging.info('Starting with QA')
        dfqa = PandasDataset(data)
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

    def _upsert_to_psql(self, data: pd.DataFrame) -> None:
        with self._psql_hook.get_sqlalchemy_engine().begin() as conn:

            # Create temporary table to insert
            assert self._meta.tables
            tbl = self._meta.tables.get(TABLE_PATH_TMP.name, None)
            assert tbl
            tbl.create(conn)

            # Insert data from one DAG run into temporary table
            rows = data.to_sql(TABLE_PATH_TMP, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

            # Insert to main table (shared_mobility_ids) with UPSERT
            query = f"""
                INSERT INTO {TABLE_PATH.name} as ins
                SELECT *
                FROM {TABLE_PATH_TMP.name}
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

    def execute(self, context: Context) -> None:

        # Load data from MongoDB
        df = self._load_data_from_mongo(context, self._config)

        # Extract JSON data to correct pandas DataFrame
        df = self._extract_data_from_mongodb_df(df)

        # Get old data
        gdf_before = self._load_data_from_past_locations(context)

        # UNION
        df = self._merge_old_and_new_data(gdf_before, df)

        # Calculate paths
        df = self._transform_and_calculate_paths(df, context)

        # QA
        self._perform_data_qa(df)

        # Load data into PSQL, using UPSERT
        self._upsert_to_psql(df)


class SharedMobilityProviderEtlOperator(SharedMobilityOperator):

    def __init__(self, meta: MetaData, config: SharedMobilityConfig, target_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self._meta = meta
        self._config = config
        self._target_conn_id = target_conn_id
        self._psql_hook = PostgresHook(self._target_conn_id)  # This could make DAG loading slow

    def _load_data_from_mongo(self, context: Context, config: SharedMobilityConfig) -> pd.DataFrame:
        with MongoHook(dag_config.mongo_conn_id).get_conn() as client:
            query = {
                'geometry.coordinates.1': {'$gt': config.pos_south, '$lt': config.pos_north},
                'geometry.coordinates.0': {'$gt': config.pos_west, '$lt': config.pos_east},
                '_meta_runtime_utc': {
                    '$gt': self.get_data_interval_start(context),
                    '$lt': self.get_data_interval_end(context)
                },
            }
            db = client.opendata
            col = db.shared_mobility
            cursor = col.find(query)
            return pd.DataFrame(list(cursor))

    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        df = (data
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

        return df

    def _load(self, data: pd.DataFrame) -> None:

        # Load to PSQL
        with PostgresHook(self._target_conn_id).get_sqlalchemy_engine().begin() as conn:
            rows = data.to_sql(TABLE_PROVIDER.name, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {table_mart_edges.name}: {rows} rows were affected.'
            )

    def execute(self, context) -> None:

        # Extract
        df = self._load_data_from_mongo(context, self._config)

        # Transform
        df = self._transform(df)

        # Load
        self._load(df)

