import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
import time
from datetime import datetime, timedelta
from src.adapters.adapter_raw_gtp import NodeAdapter
from src.adapters.adapter_utils import *
from src.db_connector import DbConnector
from airflow.decorators import dag, task


default_args = {
    'owner': 'nader',
    'retries': 2,
    'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_mantle',
    description='Load raw tx data from Mantle',
    start_date=datetime(2023, 9, 1),
    schedule_interval='30 */2 * * *'
)
def adapter_nader_super():
    @task()
    def run_nader_super():
        adapter_params = {
            'rpc': 'local_node',
            'chain': 'mantle',
            'rpc_urls': [os.getenv("MANTLE_RPC")],
            # 'max_calls_per_rpc': {
            #     os.getenv("MANTLE_RPC_1"): 50,
            #     os.getenv("MANTLE_RPC_2"): 55,
            # }
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

        # Initial load parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': 150,
            'threads': 3,
        }

        while load_params['threads'] > 0:
            try:
                adapter.extract_raw(load_params)
                break  # Break out of the loop on successful execution
            except MaxWaitTimeExceededException as e:
                print(str(e))
                
                # Reduce threads if possible, stop if it reaches 1
                if load_params['threads'] > 1:
                    load_params['threads'] -= 1
                    print(f"Reducing threads to {load_params['threads']} and retrying.")
                else:
                    print("Reached minimum thread count (1)")
                    raise e 

                # Wait for 5 minutes before retrying
                time.sleep(300)

    run_nader_super()

adapter_nader_super()
