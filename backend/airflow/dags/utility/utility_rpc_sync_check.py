import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.new_setup.rpc_sync_checker import get_chains_available, fetch_rpc_urls, activate_nodes, fetch_all_blocks, check_sync_state, deactivate_behind_nodes

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'email_on_failure': False,
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_rpc_sync_check',
    description='DAG to check if chain nodes are synchronized',
    tags=['utility', 'hourly',],
    start_date=datetime(2023, 12, 1),
    schedule_interval='35 * * * *'
)

def blockchain_sync_dag():
    @task
    def sync_check():
        db_connector = DbConnector()
        chains = get_chains_available(db_connector)
        
        for chain_name in chains:
            print(f"Processing chain: {chain_name}")
            rpc_urls = fetch_rpc_urls(db_connector, chain_name)
            # Set initial nodes as synced
            activate_nodes(db_connector, chain_name, rpc_urls)
            blocks = fetch_all_blocks(rpc_urls)
            # Check if nodes are synced
            notsynced_nodes = check_sync_state(blocks)
            print(f"Nodes not synced for chain {chain_name}:", notsynced_nodes)
            deactivate_behind_nodes(db_connector, chain_name, notsynced_nodes)
            print(f"Done processing chain: {chain_name}")
        
        print("All chains processed.")

    sync_check()

sync_dag_instance = blockchain_sync_dag()