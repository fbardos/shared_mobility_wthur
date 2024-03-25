"""
DAG to calculate paths of scooters.
Also calculates data marts for Grafana dashboard.

"""
import datetime as dt
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
import pickle
from typing import Optional

import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
import pymongo
from great_expectations.dataset import PandasDataset
from pyproj import CRS
from python_docker_operator.interface import (ConnectionInterface,
                                              ContextInterface)
from shapely.geometry import LineString, Point
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel

import sharedmobility.tables as smt


class SharedMobilityTransformation(ABC):

    def get_postgres_sqlalchemy_engine(self, conn_id: str) -> Engine:
        return ConnectionInterface(conn_id).sqlalchemy_engine

    def get_mongodb_pymongo_collection(self, conn_id: str) -> pymongo.database.Database:
        return ConnectionInterface(conn_id).mongodb_collection

    @abstractmethod
    def execute(self) -> None:
        pass


# Duplicate definition to the one in DAG
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


class CheckOrCreatePostgresTableTransformation(SharedMobilityTransformation):

    def __init__(
        self,
        table_name: str,
        target_conn_id: str,
    ):
        self._table_name = table_name
        self._target_conn_id = target_conn_id

    def execute(self):

        # Do not use PostgresHook anymore, because airflow Task gets executed
        # inside docker container instead of host with airflow.
        # Therefore, no airflow connection is available.
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)

        # If table is not existent, create table
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, self._table_name):
                SQLModel.metadata.tables[self._table_name].create(engine)


class AssertPathRowsExecutionDate(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        period_start: dt.datetime,
        period_end: dt.datetime,
        fail_if_no_rows: bool = False,
    ):
        self._target_conn_id = target_conn_id
        self._period_start = period_start
        self._period_end = period_end
        self._fail_if_no_rows = fail_if_no_rows

    def execute(self):
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
        with engine.connect() as conn:
            query = f"""
                SELECT id from {smt.TablePath.__tablename__}
                WHERE time_to BETWEEN :t_start and :t_end
            """
            count = (
                conn
                .execute(text(query), dict(t_start=self._period_start, t_end=self._period_end))
                .scalar()
            )

            if count == 0:
                msg = f'Expected rows for path table >0 for a given time interval. Rows: {rows}'
                if self._fail_if_no_rows:
                    raise Exception(msg)
                else:
                    logging.warning(msg)


class DeleteOldRowsTransformation(SharedMobilityTransformation):

    def __init__(
        self,
        table_name: str,
        column_name: str,
        target_conn_id: str,
        is_delete: bool = False,
        delete_before: Optional[dt.datetime] = None,
    ):
        self._table_name = table_name
        self._column_name = column_name
        self._target_conn_id = target_conn_id
        self._is_delete = is_delete
        self._delete_before = delete_before

    def execute(self):
        if self._is_delete:
            engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
            with engine.connect() as conn:
                query = f"""
                    DELETE FROM {self._table_name}
                    WHERE {self._column_name}::date < :delete_from
                """
                conn.execute(text(query), dict(delete_from=self._delete_before))


class PathEtlTransformation(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        config: Optional[SharedMobilityConfig] = None,
    ):
        self._config = config if config else SharedMobilityConfig()
        self._target_conn_id = target_conn_id

    def _load_data_from_mongo(self) -> pd.DataFrame:
        """Load all datapoints for the given day."""
        logging.info('Load data from MongoDB')
        mongo_db = self.get_mongodb_pymongo_collection(self._config.mongo_conn_id)
        col = mongo_db.shared_mobility
        query = {
            'geometry.coordinates.1': {'$gt': self._config.pos_south, '$lt': self._config.pos_north},
            'geometry.coordinates.0': {'$gt': self._config.pos_west, '$lt': self._config.pos_east},
            # Datetime is stored as UTC
            '_meta_runtime_utc': {
                '$gt': ContextInterface().env_data_interval_start,
                '$lt': ContextInterface().env_data_interval_end,
            },
        }
        projection = {
            '_id': 0,
            'geometry.coordinates': 1,
            'id': 1,
            '_meta_last_updated_utc': 1,
            'properties.provider.name': 1,
        }
        cursor = col.find(query, projection)
        # mongo_db.close()
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

    def _load_data_from_past_locations(self) -> gpd.GeoDataFrame:
        """
        Load the last 2 rows (t-1, t-2) of every group id and time_to to compare later with
        the first entry of the DAG execution date (t0).
        When position t-1 == t0, then the second last point (t-2) is needed to calculate
        the correct path.
        """
        logging.info('Load additional info for past locations.')
        query = f"""
            select *
                from (
                select
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY time_to desc) as r,
                    mart.id,
                    mart.provider,
                    mart.time_from as time,
                    mart.point
                from {smt.TablePath.__tablename__} mart
                where
                    time_to BETWEEN %(t_start)s - INTERVAL '30 days' AND %(t_start)s
            ) x
            where
                x.r <= 2
        """
        gdf_before = (
            gpd.GeoDataFrame.from_postgis(
                sql=query,
                con=self.get_postgres_sqlalchemy_engine(self._target_conn_id),
                geom_col='point', parse_dates='time',
                params={
                    't_start': ContextInterface().env_data_interval_start,
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
        logging.info(f'Size of table imported from PSQL {smt.TablePath.__tablename__}: {len(old.index)} rows.')
        df = (
            pd.concat([old, new], ignore_index=True)
            .set_geometry('geometry')
            .sort_values(['id', 'time'])
            .reset_index(drop=True)
        )
        return df

    def _get_graph_from_bbox(self, graph_type: str):

        # Caching: Load data from redis, when available
        _key = f'sharedmobility:osmnx:graph:{graph_type}'
        redis = ConnectionInterface(SharedMobilityConfig.redis_conn_id).redis_connection
        if redis.exists(_key):
            return pickle.loads(redis.get(_key))
        else:
            graph = ox.graph_from_bbox(
                south=self._config.pos_south, north=self._config.pos_north,
                west=self._config.pos_west, east=self._config.pos_east,
                network_type=graph_type
            )
            redis.set(_key, pickle.dumps(graph), ex=60*60*12)
            return graph


    @property
    def graph_walk(self):
        return self._get_graph_from_bbox('walk')

    @property
    def graph_bike(self):
        return self._get_graph_from_bbox('bike')

    def _transform_and_calculate_paths(self, data: pd.DataFrame) -> pd.DataFrame:
        gdf = data

        # After UNION, delete rows, where max(time_to) per scooter ID is lower than the current execution date.
        # This means, that for this particular scooter, no path has to be recalculated again.
        # Otherwise, the time consuming path calculation will be performed again.
        gdf['_time_max_scooter'] = gdf.groupby('id')['time'].transform('max')
        gdf = gdf[pd.to_datetime(gdf['_time_max_scooter'], utc=True) > ContextInterface().env_data_interval_start]
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
            .assign(point=lambda x: x['point'].to_crs(epsg=4326))
            .assign(path_walk_since_last=lambda x: x['path_walk_since_last'].to_crs(epsg=4326))
            .assign(path_bike_since_last=lambda x: x['path_bike_since_last'].to_crs(epsg=4326))
        )

        # Keep only rows with the time_to in the current DAG time interval
        gdf = gdf[gdf['time_to'] >= ContextInterface().env_data_interval_start]

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

    def _upsert_to_psql(self, data: gpd.GeoDataFrame) -> None:
        with self.get_postgres_sqlalchemy_engine(self._target_conn_id).begin() as conn:

            # Insert data from one DAG run into temporary table
            # Rows get inserted in another order in temp table than in the regular table.
            dtype_definition = {col.name: col.type for col in smt.TablePath.__table__.columns}
            table_name_tmp = f'{smt.TablePath.__tablename__}_tmp'
            rows = data.to_postgis(
                table_name_tmp,
                conn,
                index=False,
                if_exists='replace',
                dtype=dtype_definition,
            )
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {smt.TableMartEdges.__tablename__}: {rows} rows were affected.'
            )

            # Insert to main table (shared_mobility_ids) with UPSERT
            # The correct order of columns in the SELCECT statement is important
            query = f"""
                INSERT INTO {smt.TablePath.__tablename__} as ins
                SELECT
                    id
                    , provider
                    , point
                    , time_from
                    , time_to
                    , distance_m
                    , moving
                    , distance_m_walk
                    , path_walk_since_last
                    , distance_m_bike
                    , path_bike_since_last
                FROM {table_name_tmp}
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
            rows = conn.execute(text(query))
            logging.log(
                logging.WARNING if rows == 0 else logging.INFO,
                f'Table {smt.TableMartEdges.__tablename__}: {rows} rows were affected.'
            )

    def execute(self) -> None:

        # Load data from MongoDB
        df = self._load_data_from_mongo()
        
        # Extract JSON data to correct pandas DataFrame
        df = self._extract_data_from_mongodb_df(df)

        # Get old data
        gdf_before = self._load_data_from_past_locations()

        # UNION
        df = self._merge_old_and_new_data(gdf_before, df)

        # Calculate paths
        df = self._transform_and_calculate_paths(df)

        # QA
        self._perform_data_qa(df)

        # Load data into PSQL, using UPSERT
        self._upsert_to_psql(df)


class ProviderEtlTransformation(SharedMobilityTransformation):

    def __init__(self, target_conn_id: str, config: Optional[SharedMobilityConfig] = None):
        self._target_conn_id = target_conn_id
        self._config = config if config else SharedMobilityConfig()

    def _load_data_from_mongo(self) -> pd.DataFrame:
        mongodb = self.get_mongodb_pymongo_collection(self._config.mongo_conn_id)
        col = mongodb.shared_mobility
        query = {
            'geometry.coordinates.1': {'$gt': self._config.pos_south, '$lt': self._config.pos_north},
            'geometry.coordinates.0': {'$gt': self._config.pos_west, '$lt': self._config.pos_east},
            '_meta_runtime_utc': {
                '$gt': ContextInterface().env_data_interval_start,
                '$lt': ContextInterface().env_data_interval_end,
            },
        }
        cursor = col.find(query)
        return pd.DataFrame(list(cursor))

    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        df = (data
            .assign(provider=lambda x: x['properties'].apply(lambda y: y['provider']['name']))
            .assign(delta_updated=lambda x: x['_meta_runtime_utc'] - x['_meta_last_updated_utc'])
            .assign(id=lambda x: x['properties'].apply(lambda y: y['id']))
            .assign(available=1)  # After API upgrade, does not return available anymore
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
        with self.get_postgres_sqlalchemy_engine(self._target_conn_id).begin() as conn:
            rows = data.to_sql(smt.TableProvider.__tablename__, conn, index=False, if_exists='append')
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {smt.TableMartEdges.__tablename__}: {rows} rows were affected.'
            )

    def execute(self) -> None:

        # Extract
        df = self._load_data_from_mongo()

        # Transform
        df = self._transform(df)

        # Load
        self._load(df)


class IdsEtlTransformation(SharedMobilityTransformation):

    def __init__(self, target_conn_id: str, config: Optional[SharedMobilityConfig] = None):
        self._target_conn_id = target_conn_id
        self._config = config if config else SharedMobilityConfig()

    def execute(self) -> None:
        mongodb = self.get_mongodb_pymongo_collection(self._config.mongo_conn_id)
        col = mongodb.shared_mobility
        query = {
            'geometry.coordinates.1': {'$gt': self._config.pos_south, '$lt': self._config.pos_north},
            'geometry.coordinates.0': {'$gt': self._config.pos_west, '$lt': self._config.pos_east},
            '_meta_runtime_utc': {
                '$gt': ContextInterface().env_data_interval_start,
                '$lt': ContextInterface().env_data_interval_end,
            },
        }
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

        logging.info(f'Fill the table {smt.TableIds.__tablename__} with UPSERT')
        with self.get_postgres_sqlalchemy_engine(self._target_conn_id).begin() as conn:

            # Insert data from one DAG run into temporary table
            table_name_tmp = f'{smt.TableIds.__tablename__}_tmp'
            dtype_definition = {col.name: col.type for col in smt.TableIds.__table__.columns}
            rows = df.to_sql(table_name_tmp, conn, index=False, if_exists='replace', dtype=dtype_definition)
            logging.log(
                logging.WARNING if (rows := rows) is None else logging.INFO,
                f'Table {smt.TableIds.__tablename__}: {rows} rows were affected.'
            )

            # Insert to main table (shared_mobility_ids) with UPSERT
            query = f"""
                INSERT INTO {smt.TableIds.__tablename__} as ins
                SELECT *
                FROM {table_name_tmp}
                ON CONFLICT (id) DO UPDATE
                SET
                    provider = EXCLUDED.provider
                    , first_seen = LEAST(ins.first_seen, EXCLUDED.first_seen)
                    , last_seen = GREATEST(ins.last_seen, EXCLUDED.last_seen)
                    , datapoints = ins.datapoints + EXCLUDED.datapoints
                ;
            """
            rows = conn.execute(text(query))
            logging.log(
                logging.WARNING if rows == 0 else logging.INFO,
                f'Table {smt.TableIds.__tablename__}: {rows} rows were affected.'
            )


# ################################################################################################################
# Data Marts
# ################################################################################################################
class GenerateMartEdges(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        period_start: dt.datetime,
        period_end: dt.datetime,

    ):
        self._target_conn_id = target_conn_id
        self._period_start = period_start
        self._period_end = period_end

    def execute(self):
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
        with engine.connect() as conn:
            query = f"""
                INSERT INTO {smt.TableMartEdges.__tablename__}
                SELECT
                    t2.id
                    , t2.provider
                    , t2.time_from
                    , t2.time_to
                    , t2.path_idx
                    , t2.point
                    , t2.point_before
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
                        , ST_SetSRID(geom, 4326) AS point
                        , LAG(geom) OVER (PARTITION BY t.id ORDER BY t.time_to, path) AS point_before
                    FROM (
                        SELECT
                            id, provider, time_from, time_to,
                            (ST_DumpPoints(path_walk_since_last)).*
                        FROM {smt.TablePath.__tablename__}
                        WHERE
                            time_from BETWEEN :t_start AND :t_end
                    ) t
                )t2
                ON CONFLICT (id, time_from, path_idx) DO UPDATE
                SET
                    provider = EXCLUDED.provider
                    , time_to = EXCLUDED.time_to
                    , point = EXCLUDED.point
                    , point_before = EXCLUDED.point_before
                    , distance_m = EXCLUDED.distance_m;



                SELECT id from {smt.TablePath.__tablename__}
                WHERE time_to BETWEEN :t_start and :t_end
            """
            conn.execute(text(query), dict(t_start=self._period_start, t_end=self._period_end))


class GenerateMartDistinctIds(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        period_start: dt.datetime,

    ):
        self._target_conn_id = target_conn_id
        self._period_start = period_start

    def execute(self):
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
        with engine.connect() as conn:
            # TODO: This query is currently not ready for inter-day periods,
            #   because it takes only the day of period_start.
            #   --> Add an UPSERT statement for the whole day, when DAG is executed multiple times a day.
            query = f"""
                INSERT INTO {smt.TableMartDistinctIds.__tablename__}
                SELECT
                    provider
                    , date_trunc('day', time_from) as time
                    , COUNT(DISTINCT id) distinct_ids
                FROM {smt.TablePath.__tablename__}
                WHERE
                    date_trunc('day', time_from) = CAST( :t_start AS date )
                GROUP BY provider, time
                ON CONFLICT (provider, time) DO UPDATE
                SET
                    distinct_ids = EXCLUDED.distinct_ids;
            """
            conn.execute(text(query), dict(t_start=self._period_start))


class GenerateMartScooterAge(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        period_start: dt.datetime,
        period_end: dt.datetime,
    ):
        self._target_conn_id = target_conn_id
        self._period_start = period_start
        self._period_end = period_end

    def execute(self):
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
        with engine.connect() as conn:
            query = f"""
                INSERT INTO {smt.TableMartScooterAge.__tablename__}
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
                        FROM {smt.TablePath.__tablename__}
                        group by id, provider
                    ) t
                    CROSS JOIN generate_series(t.min_time + interval '1' hour, t.max_time, interval '1 hour') as g(day)
                    WHERE
                        date_trunc('hour', day) between t.min_time and t.max_time
                ) t2
                WHERE
                    time BETWEEN :t_start AND :t_end
                GROUP BY time, provider
                ORDER BY time
            """
            conn.execute(text(query), dict(t_start=self._period_start, t_end=self._period_end))


class GenerateMartTripDistance(SharedMobilityTransformation):

    def __init__(
        self,
        target_conn_id: str,
        period_start: dt.datetime,
        period_end: dt.datetime,
    ):
        self._target_conn_id = target_conn_id
        self._period_start = period_start
        self._period_end = period_end

    def execute(self):
        engine = self.get_postgres_sqlalchemy_engine(self._target_conn_id)
        with engine.connect() as conn:
            query = f"""
                INSERT INTO {smt.TableMartTripDistance.__tablename__}
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
                        FROM {smt.TablePath.__tablename__}
                    ) t
                ) t2
                WHERE
                    t2.time_from BETWEEN :t_start AND :t_end
                group by provider, id, trip_id
                ON CONFLICT (id, trip_id) DO UPDATE
                SET
                    trip_start = EXCLUDED.trip_start
                    , trip_end = EXCLUDED.trip_end
                    , trip_walk_distance_m = EXCLUDED.trip_walk_distance_m
                    , trip_bike_distance_m = EXCLUDED.trip_bike_distance_m;
            """
            conn.execute(text(query), dict(t_start=self._period_start, t_end=self._period_end))
