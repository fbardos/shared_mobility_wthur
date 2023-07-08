# Analysis: Shared Mobility Winterthur

This repo contains an analysis about the shared mobility data for the Swiss city of Winterthur.

# Links
* [General Information](https://opendata.swiss/de/dataset/standorte-und-verfugbarkeit-von-shared-mobility-angeboten)
* [REST API](https://api.sharedmobility.ch/documentation)
* [Website city of Winterthur](https://api.sharedmobility.ch/documentation)
* [Repo Shared Mobility](https://github.com/SFOE/sharedmobility)
* [GBFS Specification](https://github.com/MobilityData/gbfs)

## Structure (WIP)
* `analysis/`: Contains Jupyter Notebooks and utils
    * Part 1: General Overview: 1 week
    * Part 2: General Overview: all data
    * Part 3: Usage, Distance, Trips
    * Part 4: Pathfinding
    * Part 5: Long-term (> 1 week data)
* `airflow/`: Contains the DAG for the data extraction from the API.

## Data Marts
ETL is done using Apache Airflow. Data Marts are stored in PostgreSQL.
General scope: Only calculated for the area of Winterthur, even if we have all the data for Switzerland.

### Per provider and 2min timeframe
Grouping:
* timeframe (2min), using import time (not API's last updated)
* provider

Features:
* count_datapoints: count of all datapoints in the given timeframe
* count_distinct_ids: count distinct values for scooter ids
* avg_delta_updated: average time delta to last updated (time provider) --> clean time zones
* count_available: count amount of datapoints with `properties.provider.available == True`
* count_disabled: count amount of disabled datapoints
* count_reserved: count amount of reserved datapoints

### Scooter path
