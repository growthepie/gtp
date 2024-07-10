import getpass
import sys

# Add the user's directory to the system path
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.contract_loader import ContractLoader
from src.chain_config import adapter_mapping

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook,
    },
    dag_id='contract_loader_dag',
    description='Loads contract data using the ContractLoader',
    tags=['contracts', 'daily'],
    start_date=datetime(2023, 6, 5),
    schedule='05 02 * * *',
)

def load_metadata():
    
    for adapter in [adapter for adapter in adapter_mapping if adapter.load_contract_metadata == True]:
        chain = adapter.origin_key
        
        @task(task_id=f'contract_metadata_{chain}')
        def run_contract_loader(chain_name):
            db_connector = DbConnector()
            days = 2
            
            rpc_list = db_connector.get_special_use_rpc(chain_name)
            if rpc_list:  # Check if rpc_list is not empty
                rpc_urls = rpc_list  # Directly use rpc_list if it's already a list of URLs
                print(rpc_urls)
            else:
                print("No RPC URLs found.")
                return  # Exit the task if no RPC URLs are found

            adapter_params = {
                'days': days,
                'chain': chain_name,
                'rpc_urls': rpc_urls,
            }

            loader = ContractLoader(adapter_params, db_connector)

            try:
                loader.extract_raw()
            except Exception as e:
                print(e)

        run_contract_loader(chain)

load_metadata()
