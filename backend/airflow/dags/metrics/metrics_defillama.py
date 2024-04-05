from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_defillama import AdapterDefiLlama

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_defillama',
    description='Load stablecoin mcap where applicable.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='02 02 * * *'
)

def etl():
    @task()
    def run_stables():
        adapter_params = {
        }
        load_params = {
            'origin_keys' : None,
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDefiLlama(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_stables()
etl()

