import sys

from dataclasses import dataclass


def load_dataclass_instance_from_name(class_name: str):
    _module = sys.modules[__name__]
    return getattr(_module, class_name)()


class SharedMobilityConfig:
    pos_change_when_bigger_than_meter: int = 50
    mongo_conn_id: str = 'mongo_opendata'
    redis_conn_id: str = 'redis_cache'
    keep_public_days: int = 400  # We have data for about 400 days, keep all
    city_bfs_id: int = -1
    pos_south: float = 0.0
    pos_north: float = 0.0
    pos_west: float = 0.0
    pos_east: float = 0.0


class WinterthurSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 230
    pos_south: float = 47.449753
    pos_north: float = 47.532571
    pos_west: float = 8.687158
    pos_east: float = 8.784503


class ZurichSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 261
    pos_south: float = 47.306
    pos_north: float = 47.460
    pos_west: float = 8.420
    pos_east: float = 8.610


class UsterSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 198
    pos_south: float = 47.330
    pos_north: float = 47.375
    pos_west: float = 8.672
    pos_east: float = 8.757


class EffretikonSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 296
    pos_south: float = 47.410
    pos_north: float = 47.451
    pos_west: float = 8.659
    pos_east: float = 8.715


class FrauenfeldSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 4566
    pos_south: float = 47.534
    pos_north: float = 47.578
    pos_west: float = 8.825
    pos_east: float = 8.946


class StGallenSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 3203
    pos_south: float = 47.388
    pos_north: float = 47.490
    pos_west: float = 9.152
    pos_east: float = 9.443


class BaselSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 2701
    pos_south: float = 47.465
    pos_north: float = 47.623
    pos_west: float = 7.489
    pos_east: float = 7.703


class BielSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 371
    pos_south: float = 47.112
    pos_north: float = 47.174
    pos_west: float = 7.203
    pos_east: float = 7.322


class GrenchenSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 2546
    pos_south: float = 47.172
    pos_north: float = 47.217
    pos_west: float = 7.362
    pos_east: float = 7.440


class BernSharedMobilityConfig(SharedMobilityConfig):
    city_bfs_id: int = 351
    pos_south: float = 46.900
    pos_north: float = 47.000
    pos_west: float = 7.329
    pos_east: float = 7.542
