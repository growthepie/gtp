import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.new_setup.adapter import NodeAdapter
from src.db_connector import DbConnector
from src.new_setup.utils import Web3CC
from src.adapters.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day, find_last_block_of_day

## DAG Configuration Variables
# batch_size: Number of blocks to process in a single task run
# config: Environment variable containing the RPC node configuration
# active: Whether the chain is active and should be backfilled

chain_settings = {
    'blast': {'batch_size': 10, 'config': 'BLAST_CONFIG', 'active': True},
    'ethereum': {'batch_size': 3, 'config': 'ETH_CONFIG', 'active': True},
}

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
    },
    dag_id='backfill_rpc',
    description='DAG for backfilling missing blockchain data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 12, 1),
    schedule_interval='20 11 * * *'
)
def backfiller_dag():
    for chain, settings in chain_settings.items():
        if settings['active']:
            @task(task_id=f'new_backfill_{chain}')
            def run_backfill_task(chain_name, env_var, db_connector, start_date, end_date, batch_size):
                config = os.getenv(env_var)
                rpc_configs = json.loads(config)
                w3 = None

                for rpc_config in rpc_configs:
                    try:
                        w3 = Web3CC(rpc_config)
                        break
                    except Exception as e:
                        print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")

                if not w3:
                    raise ConnectionError("Failed to connect to any provided RPC node.")

                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                start_timestamp = date_to_unix_timestamp(start_date_obj.year, start_date_obj.month, start_date_obj.day)
                end_timestamp = date_to_unix_timestamp(end_date_obj.year, end_date_obj.month, end_date_obj.day)
                start_block = find_first_block_of_day(w3, start_timestamp)
                end_block = find_last_block_of_day(w3, end_timestamp)

                adapter_params = {'chain': chain_name, 'rpc_configs': rpc_configs}
                node_adapter = NodeAdapter(adapter_params, db_connector)

                try:
                    node_adapter.backfill_missing_blocks(start_block, end_block, batch_size)
                    print("Backfill process completed successfully.")
                except Exception as e:
                    print(f"Backfiller: An error occurred: {e}", file=sys.stderr)
                    raise

            env_var = settings['config']
            batch_size = settings['batch_size']
            db_connector = DbConnector()

            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            run_backfill_task(chain_name=chain, env_var=env_var, db_connector=db_connector, start_date=start_date, end_date=end_date, batch_size=batch_size)

backfiller_dag_instance = backfiller_dag()
