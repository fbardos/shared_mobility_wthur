import argparse
import logging
import sys
import sharedmobility.transformation as sm
from python_docker_operator.interface import ContextInterface


if __name__ == '__main__':
    
    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('target_conn_id', type=str)
    parser.add_argument('--failifnorows', action='store_true')
    logging.debug(f'Running python script with args: {sys.argv}')
    
    # Run
    args = parser.parse_args()
    op = sm.AssertPathRowsExecutionDate(
        target_conn_id=args.target_conn_id,
        period_start=ContextInterface().env_data_interval_start,
        period_end=ContextInterface().env_data_interval_end,
        fail_if_no_rows=args.failifnorows
    )
    op.execute()

