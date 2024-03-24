import argparse
import datetime as dt
import logging
import sys

from python_docker_operator.interface import ContextInterface

import sharedmobility.transformation as sm

if __name__ == '__main__':

    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('table_name', type=str)
    parser.add_argument('column_name', type=str)
    parser.add_argument('target_conn_id', type=str)
    parser.add_argument('--isdelete', action='store_true')
    parser.add_argument('--deletebeforedays', type=int)
    logging.debug(f'Running python script with args: {sys.argv}')

    # Run
    args = parser.parse_args()
    op = sm.DeleteOldRowsTransformation(
        table_name=args.table_name,
        column_name=args.column_name,
        target_conn_id=args.target_conn_id,
        is_delete=args.isdelete,
        delete_before=ContextInterface().env_data_interval_start - dt.timedelta(days=args.deletebeforedays),
    )
    op.execute()

