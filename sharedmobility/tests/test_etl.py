import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import pytest
from osmnx.io import load_graphml
from pandas.api.types import is_string_dtype
from sqlalchemy import MetaData

from airflow.models import DagBag
from airflow.utils.context import Context

import sharedmobility.shared_mobility_wthur as sm

# Important when importing local files:
#   Pytest comes up with this test package name by finding the first directory at or above the level of the file that
#   does not include an __init__.py file and declaring that the "basedir" for the module tree containing a module
#   generated from this file. It then adds the basedir to sys.path and imports using the module name that will find
#   that file relative to the basedir.
#   Source: https://stackoverflow.com/a/50169991
from sharedmobility.shared_mobility_wthur import (
    SharedMobilityConfig, SharedMobilityPathEtlOperator)
from sharedmobility.docker import ContextVariables


TEST_TASK_ID = 'path_etl'


@pytest.fixture(scope='class')
def monkeyclass():
    """Patches objects with monkeypatch on class scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


def get_path_testdata(filename: str) -> str:
    return os.path.join(Path(__file__).parents[0], 'data', filename)


@pytest.fixture(scope='session')
def data_path_simple():
    return pd.read_json(get_path_testdata('test_path_single_id.json'))


@pytest.fixture(scope='session')
def data_path_double():
    return pd.read_json(get_path_testdata('test_path_double.json'))



@pytest.fixture(scope='session')
def data_path_standing():
    return pd.read_json(get_path_testdata('test_path_standing_still.json'))



@pytest.fixture
def dagbag():
    return DagBag()


class TestPathEtl:
    # TODO: Separate trips (merge moving=True) is not tested here. Add another test for this.
    # This test only teststhe separate moves.

    @dataclass
    class OperatorContainer:
        operator: SharedMobilityPathEtlOperator
        dummy_context: Context
        data: Optional[pd.DataFrame] = None
    
    @pytest.fixture(scope='class')
    def path_etl_operator(self, monkeyclass) -> OperatorContainer:
        
        def mocked_data_interval_start(*args, **kwargs) -> dt.datetime:
            return dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'get_data_interval_start', mocked_data_interval_start)
        monkeyclass.setattr(ContextVariables, 'get_env_interval_start', property(mocked_data_interval_start))

        # meta = MetaData()
        # config = SharedMobilityConfig()  # use default values
        op = SharedMobilityPathEtlOperator(
            # task_id=TEST_TASK_ID,  --> REMOVED because not a Airflow BaseOperator anymore
            # meta=meta,
            # config=config,
            target_conn_id='dummy',
        )
        dummy_context = Context()
        return self.OperatorContainer(op, dummy_context)

    @pytest.fixture(scope='class')
    def graph_walk(self):
        return load_graphml(get_path_testdata('graph_walk_2023-11-11.graphml.xml'))

    @pytest.fixture(scope='class')
    def graph_bike(self):
        return load_graphml(get_path_testdata('graph_bike_2023-11-11.graphml.xml'))

    @pytest.fixture(scope='class')
    def data_simple(self, path_etl_operator, data_path_simple) -> OperatorContainer:
        path_etl_operator.data = path_etl_operator.operator._extract_data_from_mongodb_df(data_path_simple)
        return path_etl_operator
    
    @pytest.fixture(scope='class')
    def data_double(self, path_etl_operator, data_path_double) -> OperatorContainer:
        path_etl_operator.data = path_etl_operator.operator._extract_data_from_mongodb_df(data_path_double)
        return path_etl_operator
    
    @pytest.fixture(scope='class')
    def data_standing(self, path_etl_operator, data_path_standing) -> OperatorContainer:
        path_etl_operator.data = path_etl_operator.operator._extract_data_from_mongodb_df(data_path_standing)
        return path_etl_operator

    @pytest.fixture(scope='class')
    def calculated_path(self, monkeyclass, data_simple, graph_walk, graph_bike) -> pd.DataFrame:
        # TODO: DRY: Find a way to not write duplicate monkeypatches for multiple fixtures...
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_walk', graph_walk)
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_bike', graph_bike)

        # Calculate actual paths (can take some time)
        return data_simple.operator._transform_and_calculate_paths(
            data_simple.data
        )
    
    @pytest.fixture(scope='class')
    def calculated_path_double(self, monkeyclass, data_double, graph_walk, graph_bike) -> pd.DataFrame:
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_walk', graph_walk)
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_bike', graph_bike)

        # Calculate actual paths (can take some time)
        return data_double.operator._transform_and_calculate_paths(
            data_double.data
        )
    
    @pytest.fixture(scope='class')
    def calculated_path_standing(self, monkeyclass, data_standing, graph_walk, graph_bike) -> pd.DataFrame:
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_walk', graph_walk)
        monkeyclass.setattr(SharedMobilityPathEtlOperator, 'graph_bike', graph_bike)


        # Calculate actual paths (can take some time)
        return data_standing.operator._transform_and_calculate_paths(
            data_standing.data
        )

    @pytest.mark.parametrize(
        'data, expected_type',
        [
            ('calculated_path', pd.DataFrame),
            ('calculated_path_double', pd.DataFrame),
            ('calculated_path_standing', gpd.GeoDataFrame)
        ],
    )
    def test_dataframe_output(self, data, expected_type, request):
        assert isinstance(request.getfixturevalue(data), expected_type)
    
    @pytest.mark.parametrize(
        'data, expected_len',
        [
            ('calculated_path', 3),
            ('calculated_path_double', 6),
            ('calculated_path_standing', 0)
        ],
    )
    def test_observations(self, data: str, expected_len: int, request: pytest.FixtureRequest):
        assert len(request.getfixturevalue(data).index) == expected_len

    @pytest.mark.parametrize(
        'data, expected_providers',
        [
            ('calculated_path', 1),
            ('calculated_path_double', 2),
        ],
    )
    def test_amount_providers(self, data: str, expected_providers: int, request: pytest.FixtureRequest):
        df = request.getfixturevalue(data)
        assert len(df['provider'].value_counts().to_dict()) == expected_providers

    @pytest.mark.parametrize(
        'data, provider_name, expected_points',
        [
            ('calculated_path', 'Bolt', 3),
            ('calculated_path_double', 'Bolt', 3),
            ('calculated_path_double', 'Voi', 3),
        ],
    )
    def test_points_per_provider(
        self, data: str, provider_name: str, expected_points: int, request: pytest.FixtureRequest
    ):
        df = request.getfixturevalue(data)
        providers = df['provider'].value_counts().to_dict()
        assert providers.get(provider_name, None) == expected_points

    @pytest.mark.parametrize(
        'data, expected_moving, expected_not_moving',
        [
            ('calculated_path', 1, 2),
            ('calculated_path_double', 2, 4),
        ],
    )
    def test_moving(self, data: str, expected_moving: int, expected_not_moving: int, request: pytest.FixtureRequest):
        df = request.getfixturevalue(data)
        assert len(df[df['moving']].index) == expected_moving
        assert len(df[~df['moving']].index) == expected_not_moving

    @pytest.mark.parametrize(
        'data, time_min, time_max',
        [
            (
                'calculated_path',
                dt.datetime(2023, 1, 1, 1, 41, 15, tzinfo=dt.timezone.utc),
                dt.datetime(2023, 1, 1, 2, 40, 15, tzinfo=dt.timezone.utc),
            ),
            (
                'calculated_path_double',
                dt.datetime(2023, 1, 1, 1, 41, 15, tzinfo=dt.timezone.utc),
                dt.datetime(2023, 1, 1, 2, 40, 15, tzinfo=dt.timezone.utc),
            ),

        ],
    )
    def test_time_min_max(self, data: str, time_min: dt.datetime, time_max: dt.datetime, request: pytest.FixtureRequest):
        df = request.getfixturevalue(data)
        assert min(df['time_from']) == time_min
        assert max(df['time_to']) == time_max

    @pytest.mark.parametrize('column_name', ['path_walk_since_last', 'path_bike_since_last'])
    def test_dataframe_columns(self, calculated_path, column_name: str):
        """Expect dtype object

        To insert into PostGIS, geometry columns must be stored as 
        strings (WKT), not as geometry.
        """
        assert is_string_dtype(calculated_path[column_name])

    @pytest.mark.parametrize(
        'step, exp_lower, exp_upper, err_factor',
        [
            (0, 500, 600, 1.8), (1, 500, 600, 1.8), (2, 300, 400, 1.8)
        ]
    )
    def test_path(self, calculated_path, step: int, exp_lower: int, exp_upper:int, err_factor:int):
        df = calculated_path

        # air distance
        assert exp_lower <= df['distance_m'].iloc[step] <= exp_upper

        # walk/bike distance
        for t in ('walk', 'bike'):
            l = exp_lower
            u = exp_upper
            assert l * (1/err_factor) <= df[f'distance_m_{t}'].iloc[step] <= u * err_factor

# Maybe not needed anymore...
def test_sqlmodel_tables():
    dbbase = sm.SQLModel
    actual_table_names = list(dbbase.metadata.tables.keys())
    expected_tables = [
        'shared_mobility_path',
        'shared_mobility_path_tmp',
        'shared_mobility_provider',
        'shared_mobility_ids',
        'shared_mobility_ids_tmp',
        'shared_mobility_mart_edges',
    ]
    for i in expected_tables:
        assert i in actual_table_names
    assert 1 == 2


