import argparse
import logging
import sys
import sharedmobility.transformation as sm


if __name__ == '__main__':
    
    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('table_name', type=str)
    parser.add_argument('target_conn_id', type=str)
    logging.debug(f'Running python script with args: {sys.argv}')

    # Run
    args = parser.parse_args()
    op = sm.CheckOrCreatePostgresTableTransformation(
        table_name=args.table_name,
        target_conn_id=args.target_conn_id,
    )
    op.execute()

