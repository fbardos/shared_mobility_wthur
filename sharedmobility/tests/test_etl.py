import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geopandas as gpd
from geopandas.array import GeometryDtype
import pandas as pd
import pytest
from osmnx.io import load_graphml
from networkx import MultiDiGraph
from airflow.models import DagBag

import sharedmobility.tables as smt
import sharedmobility.transformation as sm
from sharedmobility.config import load_dataclass_instance_from_name

# Important when importing local files:
#   Pytest comes up with this test package name by finding the first directory at or above the level of the file that
#   does not include an __init__.py file and declaring that the "basedir" for the module tree containing a module
#   generated from this file. It then adds the basedir to sys.path and imports using the module name that will find
#   that file relative to the basedir.
#   Source: https://stackoverflow.com/a/50169991


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
        operator: sm.PathEtlTransformation
        data: Optional[pd.DataFrame] = None

    @pytest.fixture(scope='class')
    def path_etl_operator(self, monkeyclass) -> OperatorContainer:

        # Path ENV variables
        monkeyclass.setenv(
            'AIRFLOW_CONTEXT__CONTEXT__DATA_INTERVAL_START',
            '2023-01-01T00:05:00+00:00'
        )
        monkeyclass.setenv(
            'AIRFLOW_CONTEXT__CONTEXT__DATA_INTERVAL_END',
            '2023-01-02T00:05:00+00:00'
        )

        op = sm.PathEtlTransformation(
            target_conn_id='dummy',
            config=load_dataclass_instance_from_name('WinterthurSharedMobilityConfig'),
        )
        return self.OperatorContainer(op)

    @property
    def graph_walk(self) -> MultiDiGraph:
        return load_graphml(get_path_testdata('graph_walk_2023-11-11.graphml.xml'))

    @property
    def graph_bike(self) -> MultiDiGraph:
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
    def _patch_graphs(self, monkeyclass) -> None:
        monkeyclass.setattr(sm.PathEtlTransformation, 'graph_walk', self.graph_walk)
        monkeyclass.setattr(sm.PathEtlTransformation, 'graph_bike', self.graph_bike)

    @pytest.fixture(scope='class')
    def calculated_path(self, data_simple, _patch_graphs) -> pd.DataFrame:

        # Calculate actual paths (can take some time)
        return data_simple.operator._transform_and_calculate_paths(
            data_simple.data
        )

    @pytest.fixture(scope='class')
    def calculated_path_double(self, data_double, _patch_graphs) -> pd.DataFrame:

        # Calculate actual paths (can take some time)
        return data_double.operator._transform_and_calculate_paths(
            data_double.data
        )

    @pytest.fixture(scope='class')
    def calculated_path_standing(self, data_standing, _patch_graphs) -> pd.DataFrame:

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
        """Expect dtype object to be a geometry dtype."""
        assert isinstance(calculated_path[column_name].dtype, GeometryDtype)

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


def test_sqlmodel_tables():
    dbbase = smt.SQLModel
    actual_table_names = list(dbbase.metadata.tables.keys())
    expected_tables = [
        'shared_mobility_path',
        'shared_mobility_provider',
        'shared_mobility_ids',
        'shared_mobility_mart_edges',
        'shared_mobility_mart_distinct_ids',
        'shared_mobility_mart_scooter_age',
        'shared_mobility_mart_trip_distance',
    ]
    for i in expected_tables:
        assert i in actual_table_names
