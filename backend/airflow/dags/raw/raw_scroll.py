import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
import time
from datetime import datetime, timedelta
from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.funcs_rps_utils import MaxWaitTimeExceededException
from src.db_connector import DbConnector
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='raw_scroll',
    description='Load raw tx data from Scroll',
    tags=['raw', 'near-real-time', 'rpc'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='*/15 * * * *'
)

def adapter_rpc():
    @task(execution_timeout=timedelta(minutes=45))
    def run_scroll():
        adapter_params = {
            'rpc': 'local_node',
            'chain': 'scroll',
            'rpc_urls': [os.getenv("SCROLL_RPC")],
            # 'max_calls_per_rpc': {
            #     os.getenv("SCROLL_RPC"): 3,
            #     #os.getenv("SCROLL_RPC_2"): 3,
            #     # os.getenv("SCROLL_RPC_3"): 3,
            # }
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

        # Initial load parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': 20,
            'threads': 5,
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

    run_scroll()
adapter_rpc()