from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_chainbase import AdapterChainbaseRaw


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_raw_chainbase_v02',
    description = 'Load raw arbitrum transaction data',
    start_date = datetime(2023,6,5),
    schedule = '00 */2 * * *'
)

def etl():
    @task()
    def run_arbitrum():
        import os
        adapter_params = {
            'api_key' : os.getenv("CHAINBASE_API")
        }
        load_params = {
            'keys' : ['arbitrum_tx'],
            'block_start' : 'auto', ## 'auto' or a block number as int
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterChainbaseRaw(adapter_params, db_connector)
        # extract & load incremmentally
        ad.extract_raw(load_params)

    # @task()
    # def run_optimism():
    #     import os
    #     adapter_params = {
    #         'api_key' : os.getenv("CHAINBASE_API")
    #     }
    #     load_params = {
    #         'keys' : ['optimism_tx'],
    #         'block_start' : 'auto', ## 'auto' or a block number as int
    #     }

    #    # initialize adapter
    #     db_connector = DbConnector()
    #     ad = AdapterChainbaseRaw(adapter_params, db_connector)
    #     # extract & load incremmentally
    #     ad.extract_raw(load_params)

    run_arbitrum()
    #run_optimism()

etl()