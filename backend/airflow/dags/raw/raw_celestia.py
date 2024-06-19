import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from src.adapters.adapter_celestia import AdapterCelestia
from src.db_connector import DbConnector
from src.new_setup.utils import MaxWaitTimeExceededException, get_chain_config
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
    dag_id='celestial_raw_mode',
    description='Load raw tx data from Celestia',
    tags=['raw', 'celestial', 'near-real-time', 'rpc', 'new-setup'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='*/15 * * * *'
)
def adapter_rpc():
    @task(execution_timeout=timedelta(minutes=45))
    def run_celestia():

        # Initialize DbConnector
        db_connector = DbConnector()
        
        chain_name = 'celestia'

        rpc_list, batch_size = get_chain_config(db_connector, chain_name)
        rpc_urls = [rpc['url'] for rpc in rpc_list]

        print(f"TIA_CONFIG={rpc_list}")

        adapter_params = {
            'rpc': 'local_node',
            'chain': chain_name,
            'rpc_list': rpc_urls,
        }

        # Initialize AdapterCelestia
        adapter = AdapterCelestia(adapter_params, db_connector)

        # Test run method parameters
        load_params = {
            'block_start': 'auto',
            'batch_size': batch_size,
        }

        try:
            adapter.extract_raw(load_params)
        except MaxWaitTimeExceededException as e:
            print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
            raise e

    run_celestia()

adapter_rpc()
