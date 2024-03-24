import requests
import asyncio
import aiohttp
import json
import itertools
import datetime as dt

from aiohttp.client_exceptions import ContentTypeError

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook

from python_docker_operator.interface import ConnectionInterface


class SharedMobilityExtractor:
    BASE_URL = 'https://api.sharedmobility.ch/v1/sharedmobility/'
    PROVIDERS_PATH = 'providers'

    def __init__(self, mongo_conn_id: str):
        self._mongo_conn_id = mongo_conn_id

    async def _send_request_provider(self, session, provider: str):
        scooters_provider = []
        page = 0
        while True:
            params = {
                'filters': f'ch.bfe.sharedmobility.provider.id={provider}',
                'geometryFormat': 'geojson',
                'offset': page * 50,
            }
            print(f'GET PARAMS: {params}')
            async with session.get(self.BASE_URL + 'find', params=params) as response:
                try:
                    scooter_page = await response.json()
                except ContentTypeError:
                    scooter_page = []
            if len(scooter_page) == 0:  # When end of pagination reached
                break
            else:
                page += 1
                scooters_provider.extend(scooter_page)
        return scooters_provider

    async def _get_providers_and_scooters(self):
        runtime = dt.datetime.now(tz=dt.timezone.utc)
        params = {}
        r = requests.get(self.BASE_URL + 'providers', params=params)
        providers = json.loads(r.content)
        providers_filtered = [i['provider_id'] for i in providers if 'E-Scooter' in i.get('vehicle_type', [])]

        # Extract all scooters by provider
        tasks = []
        async with aiohttp.ClientSession() as session:
            for provider in providers_filtered:
                tasks.append(self._send_request_provider(session, provider))
            scooters = await asyncio.gather(*tasks, return_exceptions=False)

        scooters = list(itertools.chain(*scooters))

        last_updated = {i['provider_id']: i['last_updated'] for i in providers}

        # Postprocessing
        for scooter in scooters:
            scooter.update({'_meta_runtime_utc': runtime})
            scooter.update({'_meta_last_updated_utc': dt.datetime.fromtimestamp(last_updated[scooter['properties']['provider']['id']], tz=dt.timezone.utc)})
            scooter['properties']['provider'].pop('apps', None)

        return scooters

    def execute(self):
        scooters = asyncio.run(self._get_providers_and_scooters())
        col = ConnectionInterface(self._mongo_conn_id).mongodb_collection.shared_mobility
        col.insert_many(scooters)
