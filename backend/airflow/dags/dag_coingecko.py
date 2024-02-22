from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_coingecko import AdapterCoingecko


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_coingecko_v01',
    description = 'Load price, volume, and market_cap from coingecko API for all tracked tokens.',
    start_date = datetime(2023,4,24),
    schedule = '15 02 * * *'
)

def etl():
    @task()
    def run_market_chart():
        adapter_params = {
        }
        load_params = {
            'load_type' : 'project',
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'origin_keys' : None, # could also be a list, see all options in adapter_mapping.py
            'days' : 'auto', # auto, max, or a number (as string)
            'vs_currencies' : ['usd', 'eth']
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_imx_tokens():
            adapter_params = {
            }
            load_params = {
                'load_type' : 'imx_tokens'
            }

            # initialize adapter
            db_connector = DbConnector()
            ad = AdapterCoingecko(adapter_params, db_connector)
            # extract
            df = ad.extract(load_params)
            # load
            ad.load(df)
    
    run_market_chart()
    run_imx_tokens()

etl()