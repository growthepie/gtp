from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_coingecko import AdapterCoingecko

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_coingecko_hourly',
    description='Load price, volume, and market_cap from coingecko API for specific tokens.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2023,4,24),
    schedule='20 * * * *'
)

def etl():
    @task()
    def run_market_chart():
        adapter_params = {
        }
        load_params = {
            'load_type' : 'project',
            'granularity' : 'hourly', # 'daily' or 'hourly
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'origin_keys' : ['ethereum', 'starknet', 'mantle'], # list of tokens that is required on hourly granularity
            'days' : '3', # auto, max, or a number (as string)
            'vs_currencies' : ['usd', 'eth']
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_market_chart()
etl()