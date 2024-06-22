import argparse
import datetime as dt
import logging
import sys

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
    op = sm.PathEtlTransformation(
        target_conn_id=args.target_conn_id,
        config=load_dataclass_instance_from_name(args.config_class_name),
    )
    op.execute()

