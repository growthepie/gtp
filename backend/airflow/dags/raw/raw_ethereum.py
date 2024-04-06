import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from datetime import datetime, timedelta
from src.new_setup.adapter import NodeAdapter
from src.new_setup.utils import MaxWaitTimeExceededException
from src.db_connector import DbConnector
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
import json

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='raw_ethereum',
    description='Load raw tx data from ethereum',
    tags=['raw', 'near-real-time', 'rpc', 'new-setup'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='*/15 * * * *'
)

def adapter_rpc():
    @task()
    def run_ethereum():

        config = os.getenv("ETH_CONFIG")

        rpc_configs = json.loads(config)

        adapter_params = {
            'rpc': 'local_node',
            'chain': 'ethereum',
            'rpc_configs': rpc_configs,
        }
        
        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize NodeAdapter
        adapter = NodeAdapter(adapter_params, db_connector)

        # Initial load parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': 3,
        }

        try:
            adapter.extract_raw(load_params)
        except MaxWaitTimeExceededException as e:
            print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
            raise e

    run_ethereum()
adapter_rpc()