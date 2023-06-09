from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_dune import AdapterDune


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_dune_v01',
    description = 'Load aggregates metrics such as txcount, daa, fees paid, stablecoin mcap.',
    start_date = datetime(2023,6,5),
    schedule = '05 02 * * *'
)

def etl():
    @task()
    def run_aggregates():
        import os
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 'auto',
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_aggregates()

etl()
