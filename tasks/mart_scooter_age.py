import argparse
import logging
import sys

from python_docker_operator.interface import ContextInterface

import sharedmobility.transformation as sm
from sharedmobility.config import load_dataclass_instance_from_name


if __name__ == '__main__':

    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('target_conn_id', type=str)
    parser.add_argument('config_class_name', type=str)
    logging.debug(f'Running python script with args: {sys.argv}')

    # Run
    args = parser.parse_args()
    op = sm.GenerateMartScooterAge(
        target_conn_id=args.target_conn_id,
        city_bfs_id=load_dataclass_instance_from_name(args.config_class_name).city_bfs_id,
        period_start=ContextInterface().env_data_interval_start,
        period_end=ContextInterface().env_data_interval_end,
    )
    op.execute()

