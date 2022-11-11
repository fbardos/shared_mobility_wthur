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
    * Part 1: General Overview
    * Part 2: Usage, Distance, Trips
    * Part 3: Pathfinding
    * Part 4: Long-term (> 1 week data)
* `airflow/`: Contains the DAG for the data extraction from the API.
