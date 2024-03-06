from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_rpc import AdapterRPCRaw

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='raw_rpc',
    description='Load raw tx data for Base and Optimism from RPC.',
    tags=['archived', 'raw', 'near-real-time', 'rpc', 'base', 'optimism'],
    start_date=datetime(2023,8,11),
    schedule_interval='*/15 * * * *'
)

def adapter_rpc():
    @task()
    def run_base():
        import os
        adapter_params = {
            'rpc': 'ankr',
            'api_key' : os.getenv("ANKR_API"),
            'chain' : 'base'
        }
        load_params = {
            'block_start' : 'auto', ## 'auto' or a block number as int
            #'block_start' : 9137631, ## 'auto' or a block number as int
            'batch_size' : 25,
            'threads' : 1
        }

       # initialize adapter
        db_connector = DbConnector()
        # initialize adapter
        ad = AdapterRPCRaw(adapter_params, db_connector)
        # extract
        ad.extract_raw(load_params)

    @task()
    def run_optimism():
        import os
        adapter_params = {
            'rpc': 'ankr',
            'api_key' : os.getenv("ANKR_API"),
            'chain' : 'optimism'
        }
        load_params = {
            'block_start' : 'auto', ## 'auto' or a block number as int
            #'block_start' : 9137631, ## 'auto' or a block number as int
            'batch_size' : 25,
            'threads' : 1
        }

       # initialize adapter
        db_connector = DbConnector()
        # initialize adapter
        ad = AdapterRPCRaw(adapter_params, db_connector)
        # extract
        ad.extract_raw(load_params)

    run_base()
    run_optimism()
adapter_rpc()