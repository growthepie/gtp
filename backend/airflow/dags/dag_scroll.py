import getpass
import os
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from src.adapters.adapter_raw_gtp import NodeAdapter, MaxWaitTimeExceededException
from src.db_connector import DbConnector
from airflow.decorators import dag, task 

default_args = {
    'owner' : 'nader',
    'retries' : 2,
    'email' : ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_scroll',
    description = 'Load raw tx data from Scroll',
    start_date = datetime(2023,11,1),
    schedule = '25 */3 * * *'
)

def adapter_nader_super():
    @task()
    def run_nader_super():
        adapter_params = {
            'rpc': 'local_node',
            'chain': 'scroll',
            'node_url': os.getenv("SCROLL_RPC"),
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

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
        load_params = {
            'block_start': 'auto',
            'batch_size': 200,
            'threads': 2,
        }
        try:
            adapter.extract_raw(load_params)
        except MaxWaitTimeExceededException as e:
            print(str(e))
            raise e 
        
    run_nader_super()

adapter_nader_super()
