import argparse
import datetime as dt
import logging
import sys

from sharedmobility.extract import SharedMobilityExtractor

if __name__ == '__main__':

    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('mongo_conn_id', type=str)
    logging.debug(f'Running python script with args: {sys.argv}')

    # Run
    args = parser.parse_args()
    op = SharedMobilityExtractor(
        mongo_conn_id=args.mongo_conn_id,
    )
    op.execute()

