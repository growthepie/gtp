from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_raw_zettablock import AdapterZettaBlockRaw

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='raw_zettablock',
    description='Load raw polygon_zkevm & zksync era transaction data',
    tags=['raw', 'near-real-time', 'zksync_era'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/15 * * * *'
)

def adapter_raw_zetta():
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
        ad.extract_raw(load_params)

    run_zksync_era()
adapter_raw_zetta()