import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
import time
from datetime import datetime, timedelta
from src.adapters.adapter_raw_gtp import NodeAdapter
from src.adapters.adapter_utils import MaxWaitTimeExceededException
from src.db_connector import DbConnector
from airflow.decorators import dag, task

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
        'email_on_failure': True,
        'retry_delay': timedelta(minutes=5)
    },
    dag_id='raw_metis',
    description='Load raw tx data from Metis',
    tags=['raw', 'near-real-time', 'rpc'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='*/15 * * * *'
)

def adapter_rpc():
    @task()
    def run_metis():
        adapter_params = {
            'rpc': 'local_node',
            'chain': 'metis',
            'rpc_urls': [os.getenv("METIS_RPC")],
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

        # Initial load parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': 150,
            'threads': 2,
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

    run_metis()
adapter_rpc()