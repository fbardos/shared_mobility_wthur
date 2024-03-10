import datetime as dt
from typing import Any, Optional

from geoalchemy2 import Geometry
from sqlalchemy import Boolean, Column, DateTime, Float, Index, Integer, String
from sqlmodel import Column, Field, SQLModel
from sqlmodel.main import SQLModelMetaclass


# Temporary FIX for the wrong order when working with direct sa_column definitions
# Source: https://github.com/tiangolo/sqlmodel/issues/542
class ColumnCloningMetaclass(SQLModelMetaclass):
    def __setattr__(cls, name: str, value: Any) -> None:
        if isinstance(value, Column):
            return super().__setattr__(name, value.copy())
        return super().__setattr__(name, value)


# Table definitions (sqlmodel)
class TablePath(SQLModel, table=True, metaclass=ColumnCloningMetaclass):

    __tablename__ = 'shared_mobility_path'

    id: str = Field(String, primary_key=True)
    provider: str = Field(String, index=True)
    point: Any = Field(sa_column=Column(Geometry('POINT')))
    time_from: dt.datetime = Field(DateTime, primary_key=True, index=True)
    time_to: dt.datetime = Field(DateTime, index=True)
    distance_m: float = Field(Float)
    moving: bool = Field(Boolean)
    distance_m_walk: Optional[float] = Field(Float)
    path_walk_since_last: Optional[Any] = Field(sa_column=Column(Geometry('LINESTRING')))
    distance_m_bike: Optional[float] = Field(Float)
    path_bike_since_last: Optional[Any] = Field(sa_column=Column(Geometry('LINESTRING')))


class TableProvider(SQLModel, table=True):

    __tablename__ = 'shared_mobility_provider'

    time: dt.datetime = Field(DateTime, primary_key=True)
    provider: str = Field(String, primary_key=True)
    count_datapoints: int = Field(Integer)
    count_distinct_ids: int = Field(Integer)
    avg_delta_updated: float = Field(Float)
    count_available: int = Field(Integer)
    count_disabled: int = Field(Integer)
    count_reserved: int = Field(Integer)


class TableIds(SQLModel, table=True):

    __tablename__ = 'shared_mobility_ids'

    id: str = Field(String, primary_key=True)
    provider: str = Field(String, index=True)
    first_seen: dt.datetime = Field(DateTime, index=True)
    last_seen: dt.datetime = Field(DateTime, index=True)
    datapoints: int = Field(Integer)


class TableMartEdges(SQLModel, table=True, metaclass=ColumnCloningMetaclass):

    __tablename__ = 'shared_mobility_mart_edges'

    id: str = Field(String, primary_key=True, index=True)
    provider: str = Field(String)
    time_from: dt.datetime = Field(DateTime, primary_key=True, index=True)
    time_to: dt.datetime = Field(DateTime, index=True)
    path_idx: int = Field(Integer, primary_key=True)
    point: Any = Field(sa_column=Column(Geometry('POINT')))
    point_before: Any = Field(sa_column=Column(Geometry('POINT', srid=4326)))
    distance_m: Optional[float] = Field(Float)

    # Call table arguments from sqlalchemy
    __table_args__ = (
        Index('idx_time_from_to', 'time_from', 'time_to'),
    )


class TableMartDistinctIds(SQLModel, table=True, metaclass=ColumnCloningMetaclass):

    __tablename__ = 'shared_mobility_mart_distinct_ids'

    provider: str = Field(String, primary_key=True, nullable=False)
    time: dt.datetime = Field(DateTime, primary_key=True, nullable=False, index=True)
    distinct_ids: int = Field(Integer, nullable=False)


class TableMartScooterAge(SQLModel, table=True, metaclass=ColumnCloningMetaclass):

    __tablename__ = 'shared_mobility_mart_scooter_age'

    time: dt.datetime = Field(DateTime, primary_key=True, nullable=False)
    provider: str = Field(String, primary_key=True, nullable=False)
    avg_age_days_scooter: float = Field(Float, nullable=False)


class TableMartTripDistance(SQLModel, table=True, metaclass=ColumnCloningMetaclass):

    __tablename__ = 'shared_mobility_mart_trip_distance'

    id: str = Field(String, primary_key=True, nullable=False)
    provider: str = Field(String, nullable=False, index=True)
    trip_id: str = Field(String, primary_key=True, nullable=False)
    trip_start: dt.datetime = Field(DateTime, nullable=False)
    trip_end: dt.datetime = Field(DateTime, nullable=False)
    trip_walk_distance_m: Optional[float] = Field(Float)
    trip_bike_distance_m: Optional[float] = Field(Float)
