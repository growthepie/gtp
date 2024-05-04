import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.new_setup.adapter import NodeAdapter
from src.db_connector import DbConnector
from src.new_setup.utils import Web3CC, get_chain_config
from src.misc.airflow_utils import alert_via_webhook
from src.adapters.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day, find_last_block_of_day

## DAG Configuration Variables
# batch_size: Number of blocks to process in a single task run
# config: Environment variable containing the RPC node configuration
# active: Whether the chain is active and should be backfilled

chain_settings = {
    'blast': {'batch_size': 10, 'active': True},
    'ethereum': {'batch_size': 3, 'active': True},
    'zksync_era': {'batch_size': 10, 'active': True},
    'base': {'batch_size': 10, 'active': True},
    'mantle': {'batch_size': 10, 'active': True},
    'arbitrum': {'batch_size': 30, 'active': True},
}

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'on_failure_callback': alert_via_webhook
    },
    dag_id='backfill_rpc_new',
    description='DAG for backfilling missing blockchain data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 12, 1),
    schedule_interval='20 11 * * *'
)
def backfiller_dag():
    for chain, settings in chain_settings.items():
        if settings['active']:
            @task(task_id=f'new_backfill_{chain}')
            def run_backfill_task(chain_name, db_connector, start_date, end_date, batch_size):
                rpc_configs = get_chain_config(db_connector, chain_name)
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

            batch_size = settings['batch_size']
            db_connector = DbConnector()

            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            run_backfill_task(chain_name=chain, db_connector=db_connector, start_date=start_date, end_date=end_date, batch_size=batch_size)

backfiller_dag_instance = backfiller_dag()
