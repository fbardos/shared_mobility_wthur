import requests
import asyncio
import aiohttp
import json
import itertools
import datetime as dt

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook

with DAG(
    dag_id='gather_shared_mobility',
    schedule_interval='*/2 * * * *',
    start_date=dt.datetime(2022, 9, 1),
    max_active_runs=1,
    catchup=False,
    tags=['gather', 'shared_mobility'],
) as dag:

    async def main():

        async def send_request_provider(session, provider: str):
            scooters_provider = []
            page = 0
            while True:
                params = {
                    'filters': f'ch.bfe.sharedmobility.provider.id={provider}',
                    'geometryFormat': 'geojson',
                    'offset': page * 50,
                }
                print(f'GET PARAMS: {params}')
                async with session.get(BASE_URL + 'find', params=params) as response:
                    r_text = await response.text()
                    scooter_page = json.loads(r_text)
                if len(scooter_page) == 0:  # When end of pagination reached
                    break
                else:
                    page += 1
                    scooters_provider.extend(scooter_page)
            return scooters_provider

        RUNTIME = dt.datetime.now(tz=dt.timezone.utc)

        # First, get all E-Scooter providers
        BASE_URL = 'https://api.sharedmobility.ch/v1/sharedmobility/'
        params = {}
        r = requests.get(BASE_URL + 'providers', params=params)
        providers = json.loads(r.content)
        providers_filtered = [i['provider_id'] for i in providers if 'E-Scooter' in i.get('vehicle_type', [])]

        # Extract all scooters by provider
        tasks = []
        async with aiohttp.ClientSession() as session:
            for provider in providers_filtered:
                tasks.append(send_request_provider(session, provider))
            scooters = await asyncio.gather(*tasks, return_exceptions=False)

        scooters = list(itertools.chain(*scooters))

        last_updated = {i['provider_id']: i['last_updated'] for i in providers}

        # Postprocessing
        for scooter in scooters:
            scooter.update({'_meta_runtime_utc': RUNTIME})
            scooter.update({'_meta_last_updated_utc': dt.datetime.fromtimestamp(last_updated[scooter['properties']['provider']['id']], tz=dt.timezone.utc)})
            scooter['properties']['provider'].pop('apps', None)

        return scooters

    @task(task_id='gather_data')
    def gather_data():
        scooters = asyncio.run(main())
        with MongoHook('mongodb_u1082') as client:
            client.insert_many('shared_mobility', scooters)

    t1 = gather_data()

    t1

