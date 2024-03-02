from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_defillama import AdapterDefiLlama


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'metrics_defillama',
    description = 'Load stablecoin mcap where applicable.',
    tags=['metrics', 'daily'],
    start_date = datetime(2023,4,24),
    schedule = '02 02 * * *'
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

