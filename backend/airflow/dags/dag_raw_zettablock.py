from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_zettablock import AdapterZettaBlockRaw


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_raw_zettablock_v01',
    description = 'Load raw polygon_zkevm & zksync era transaction data',
    start_date = datetime(2023,4,24),
    schedule_interval='35 */2 * * *'
)

def adapter_raw_zetta():
    @task()
    def run_polygon_zkevm():
        import os
        adapter_params = {
            'api_key' : os.getenv("ZETTABLOCK_API")
        }
        load_params = {
            'keys' : ['polygon_zkevm_tx'],
            'block_start' : 'auto', ## 'auto' or a block number as int
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterZettaBlockRaw(adapter_params, db_connector)
        # extract & load incremmentally
        df = ad.extract_raw(load_params)

    @task()
    def run_zksync_era():
        import os
        adapter_params = {
            'api_key' : os.getenv("ZETTABLOCK_API")
        }
        load_params = {
            'keys' : ['zksync_era_tx'],
            'block_start' : 'auto', ## 'auto' or a block number as int
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterZettaBlockRaw(adapter_params, db_connector)
        # extract & load incremmentally
        df = ad.extract_raw(load_params)

    run_polygon_zkevm()
    run_zksync_era()

adapter_raw_zetta()