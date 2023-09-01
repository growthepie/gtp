from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_nader import BaseNodeAdapter

default_args = {
    'owner' : 'nader',
    'retries' : 2,
    'email' : ['nader@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_adapter_nader',
    description = 'Load raw tx data from local node',
    start_date = datetime(2023,9,1),
    schedule = '00 */3 * * *'
)

def adapter():
    @task()
    def run_base_nader():
        import os
        adapter_params = {
            'rpc': 'local_node',
            'chain': 'base',
            'node_url': os.getenv("BASE_NODE"),
        }
        load_params = {
            'block_start' : 'auto', ## 'auto' or a block number as int
            'batch_size': 10,
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize BaseNodeAdapter
        adapter = BaseNodeAdapter(adapter_params, db_connector)

        # Test database connectivity
        if not adapter.check_db_connection():
            print("Failed to connect to database.")
        else:
            print("Successfully connected to database.")

        # Test S3 connectivity
        if not adapter.check_s3_connection():
            print("Failed to connect to S3.")
        else:
            print("Successfully connected to S3.")
            
        # Extract
        adapter.extract_raw(load_params)

    run_base_nader()

adapter()