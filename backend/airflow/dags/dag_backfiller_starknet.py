import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
import time
from datetime import datetime, timedelta
from src.adapters.adapter_utils import *
from src.db_connector import DbConnector
from airflow.decorators import dag, task
from src.adapters.adapter_starknet import AdapterStarknet

default_args = {
    'owner': 'nader',
    'retries': 2,
    'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_backfiller_starknet',
    description='Backfill potentially missing Starknet data',
    start_date=datetime(2023, 9, 1),
    schedule_interval='30 */2 * * *'
)
def backfill_strk():
    @task()
    def run_backfill_strk():
        adapter_params = {
            'chain': 'starknet',
            'rpc_url': os.getenv("STARKNET_RPC"),
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize StarkNetAdapter
        adapter = AdapterStarknet(adapter_params, db_connector)

        table_name = adapter_params['chain'] + "_tx"
        end_block = db_connector.get_max_block(table_name)
        start_block = end_block - 25000 # Backfill 25,000 blocks. Should be roughly 7 days of data

        # Call the backfill_missing_blocks method
        backfill_params = {
            'batch_size': 20,
            'threads': 1,
        }

        try:
            adapter.backfill_missing_blocks(start_block, end_block, backfill_params['batch_size'], backfill_params['threads'])
            print("Backfilling completed successfully.")
        except Exception as e:
            print(f"Backfilling stopped due to an exception: {e}")
            raise e

    run_backfill_strk()

backfill_strk()
