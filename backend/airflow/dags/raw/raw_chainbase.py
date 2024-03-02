from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_chainbase import AdapterChainbaseRaw

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='raw_chainbase',
    description='Load raw arbitrum transaction data',
    tags=['raw', 'near-real-time', 'arbitrum'],
    start_date=datetime(2023,6,5),
    schedule_interval='*/15 * * * *'
)

def etl():
    @task()
    def run_arbitrum():
        import os
        adapter_params = {
            'api_key' : os.getenv("CHAINBASE_API")
        }
        load_params = {
            'keys' : ['arbitrum_tx'],
            'block_start' : 'auto', ## 'auto' or a block number as int
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterChainbaseRaw(adapter_params, db_connector)
        # extract & load incremmentally
        ad.extract_raw(load_params)

    run_arbitrum()
etl()