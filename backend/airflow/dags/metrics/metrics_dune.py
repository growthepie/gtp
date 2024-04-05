from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_dune import AdapterDune

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_dune',
    description='Load aggregates metrics such as txcount, daa, fees paid, stablecoin mcap where applicable.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,6,5),
    schedule='05 02 * * *'
)

def etl():
    @task()
    def run_aggregates():
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 'auto',
            'load_type' : 'metrics'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_inscriptions():
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 1000,
            'load_type' : 'inscriptions'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_aggregates()
    run_inscriptions()
etl()