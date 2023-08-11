from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_rpc import AdapterRPCRaw


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_raw_rpc_v01',
    description = 'Load raw tx data from rpc providers',
    start_date = datetime(2023,8,11),
    schedule = '00 01 * * *'
)

def etl():
    @task()
    def run_base():
        import os
        adapter_params = {
            'rpc': 'ankr',
            'api_key' : os.getenv("ANKR_API"),
            'chain' : 'base'
        }
        load_params = {
            'block_start' : 'auto', ## 'auto' or a block number as int
            #'block_start' : 9137631, ## 'auto' or a block number as int
            'batch_size' : 25,
            'threads' : 1
        }

       # initialize adapter
        db_connector = DbConnector()
        # initialize adapter
        ad = AdapterRPCRaw(adapter_params, db_connector)
        # extract
        ad.extract_raw(load_params)

    # @task()
    # def run_optimism():
    #     import os
    #     adapter_params = {
    #         'rpc': 'ankr',
    #         'api_key' : os.getenv("ANKR_API"),
    #         'chain' : 'optimism'
    #     }
    #     load_params = {
    #         'block_start' : 'auto', ## 'auto' or a block number as int
    #         #'block_start' : 9137631, ## 'auto' or a block number as int
    #         'batch_size' : 25,
    #         'threads' : 1
    #     }

    #    # initialize adapter
    #     db_connector = DbConnector()
    #     # initialize adapter
    #     ad = AdapterRPCRaw(adapter_params, db_connector)
    #     # extract
    #     ad.extract_raw(load_params)



    run_base()
    # run_optimism()

etl()