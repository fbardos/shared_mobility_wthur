# Shared Mobility Winterthur

![E-Scooter Dashboard Winterthur](https://bardos.dev/images/dashboard_overview.png)

This repo contains ETL workflows (Airflow DAGs), analysis (Jupyter Notebooks) and some documentation about the shared mobility API for E-Scooters.

# Links
* [Overview & Links to shared mobility dashboard(s)](dashboard_overview.png)

# References
* [General Information](https://opendata.swiss/de/dataset/standorte-und-verfugbarkeit-von-shared-mobility-angeboten)
* [REST API](https://api.sharedmobility.ch/documentation)
* [Website city of Winterthur](https://api.sharedmobility.ch/documentation)
* [Repo Shared Mobility](https://github.com/SFOE/sharedmobility)
* [GBFS Specification](https://github.com/MobilityData/gbfs)

## Structure
* `analysis/`: Contains Jupyter Notebooks for one-time analysis and utils
* `airflow/`: Contains the data transformation (as DAG) for the data extraction from the API.

## Limitations
* When the data does not get updated regulary on API side, the calculation of routes maybe wrong the next time, the data gets updated.

## Documentation Data transformation
TODO

## Data Marts
ETL is done using Apache Airflow. Data Marts are stored in PostgreSQL.
General scope: Only calculated for the area of Winterthur, even if we have all the data for Switzerland. On request, I can rollout additional dashboards for all major cities in Switzerland.

