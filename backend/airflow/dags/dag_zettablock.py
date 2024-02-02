from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_zettablock import AdapterZettablock


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_zettablock_v01',
    description = 'Load aggregates metrics such as txcount, daa, fees paid.',
    start_date = datetime(2023,4,24),
    schedule = '06 03 * * *'
)

def etl():
    @task()
    def run_aggregates():
        import os
        adapter_params = {
            'api_key' : os.getenv("ZETTABLOCK_API_2")
        }
        load_params = {
            'origin_keys' : None,
            'metric_keys' : None,
            'days' : 'auto',
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterZettablock(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_aggregates()

etl()