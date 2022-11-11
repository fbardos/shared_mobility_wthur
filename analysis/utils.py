from __future__ import annotations
from dataclasses import dataclass
import datetime as dt
import pandas as pd
import geopandas as gpd
import numpy as np
import pymongo
import logging

from typing import Literal, List
from geopy.distance import geodesic
from shapely.geometry import Point

WTHUR_SOUTH = 47.449753
WTHUR_NORTH = 47.532571
WTHUR_WEST = 8.687158
WTHUR_EAST = 8.784503


class DataPoints:

    def __init__(self, data: gpd.GeoDataFrame):
        self.data = data  # Composition over inheritance

    def convert_timezone_column(self, cols: List[str], tz_from: str = 'UTC', tz_to: str = 'Europe/Zurich') -> gpd.GeoDataFrame:
        """Edits existing data."""
        gdf = self.data.copy()
        for col in cols:
            gdf[col] = gdf[col].dt.tz_localize(tz_from).dt.tz_convert(tz_to)
        self.data = gdf
        return gdf

    def add_distance_from_point(self, x: float, y: float, target_epsg: int = 21781) -> gpd.GeoDataFrame:
        """Returns distance in meters in new column distance_m from a static point.

        Gets applied to column geometry.

        Args:
            - x: Longitude of static point in target_epsg coordinates.
            - y: Latitude of static point in target_epsg coordinates.
            - target_epsg: Target CRS. Defaults to EPSG:21781 for Swiss coordinate system. Has to be in unit=metre, not degree.

        """
        gdf = self.data.copy()
        static_point = Point(x, y)
        gdf.to_crs(epsg=target_epsg, inplace=True)
        gdf['distance_m'] = gdf.distance(static_point)
        self.data = gdf
        return gdf


class Scooter:

    def __init__(self, id: str, provider: str, datapoints: DataPoints):
        self.id = id
        self.provider = provider
        self.datapoints = datapoints

    def get_distance_from_last_row(self, target_epsg: int = 21781):
        """Must be prefiletered by scooter."""
        gdf = self.datapoints.data.copy()
        gdf.to_crs(epsg=target_epsg, inplace=True)
        return gdf.distance(gdf.shift())

    def get_datapoints_moved(self, min_distance_m: float = 20, debug: bool = False):
        """Return datapoints with travel of at least x meters as specified in function parameter.

        But return also the first row to keep the original position started.

        """
        gdf = self.datapoints.data.copy()
        gdf['distance_m'] = self.get_distance_from_last_row()
        gdf['_distance_incr'] = np.where(gdf['distance_m'] >= min_distance_m, gdf['distance_m'], 0)
        gdf['_distance_cumsum'] = gdf['_distance_incr'].cumsum()
        gdf['_rank'] = gdf['_distance_cumsum'].rank(method='dense')
        gdf['time_from'] = gdf.groupby('_rank')['time_provider'].transform('nth', 0)
        gdf['time_to'] = gdf.groupby('_rank')['time_provider'].transform('nth', -1)

        gdf = gdf[(gdf['distance_m'] >= min_distance_m) | (gdf.index == 0)]
        gdf['stay_in_pos_sec'] = (gdf['time_to'] - gdf['time_from']).astype('timedelta64[s]')

        if debug:
            return gdf
        else:
            return gdf[[i for i in gdf.columns if not i.startswith('_')]]


class ScooterContainer:

    def __init__(self):
        self.datapoints = None
        self.scooters = []

    def generate_scooters(self):
        assert self.datapoints is not None
        for scooter_id in self.datapoints.data['id'].unique():
            sub_df = self.datapoints.data[self.datapoints.data['id'] == scooter_id]
            scooter = Scooter(scooter_id, sub_df.iloc[0].provider, DataPoints(sub_df))
            self.scooters.append(scooter)

    def _generate_datapoints_gdf(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs='epsg:4326')
        gdf.drop(['lat', 'lon'], axis=1, inplace=True)
        gdf = gdf.sort_values(['id', 'time_airflow']).reset_index(drop=True)
        return gdf

    def get_datapoints_mongodb(self, uri: str, **kwargs):
        """

        Will contain two time series:
            - time_provider: Timestamp, when provider last refreshed data.
            - time_airflow: Timestamp of Airflow DAG run.

        Kwargs:
            - lat: Tuple (from, to) of coordinates (lattitude, float) to filter.
            - lon: Tuple (from, to) of coordinates (longitue, float) to filter.
            - timerange: Tuple (from, to) of time to filter (datetime.datetime). Filters date imported, not airflow run.
            - id: List of scooter IDs (str) to filter.

        """

        # Prepare filters, get combined with logical AND
        filters = []
        if 'lat' in kwargs:
            filters.append({'geometry.coordinates.1': {'$gt': kwargs['lat'][0], '$lt': kwargs['lat'][1]}})
        if 'lon' in kwargs:
            filters.append({'geometry.coordinates.0': {'$gt': kwargs['lon'][0], '$lt': kwargs['lon'][1]}})
        if 'timerange' in kwargs:
            filters.append({'_meta_last_updated_utc': {'$gt': kwargs['timerange'][0], '$lt': kwargs['timerange'][1]}})
        if 'id' in kwargs:
            filters.append({'id': {'$in': kwargs['id']}})

        projection = {
            '_id': 0,
            'geometry.coordinates': 1,
            'id': 1,
            '_meta_runtime_utc': 1,
            '_meta_last_updated_utc': 1,
            'properties.provider.id': 1,
            'properties.available': 1,
            'properties.vehicle.status.disabled': 1,
            'properties.vehicle.status.reserved': 1,
        }

        logging.debug('FILTERS', filters)
        logging.debug('PROJECTION', projection)

        with pymongo.MongoClient(uri) as client:
            db = client['opendata']
            col = db['shared_mobility']
            results = list(col.find(
                {'$and': filters},
                projection,
            ))

        # Flatten result and generate dataframe
        coords = [[
            i['_meta_last_updated_utc'],
            i['_meta_runtime_utc'],
            i['id'],
            *i['geometry']['coordinates'],
            i['properties']['provider']['id'],
            i['properties']['available'],
            i['properties']['vehicle']['status']['disabled'],
            i['properties']['vehicle']['status']['reserved'],
        ] for i in results]
        cols = ['time_provider', 'time_airflow', 'id', 'lon', 'lat', 'provider', 'is_available', 'is_disabled', 'is_reserved']
        df = pd.DataFrame(coords, columns=cols)
        self.datapoints = DataPoints(self._generate_datapoints_gdf(df))

