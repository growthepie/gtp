from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_sql import AdapterSQL


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_sql_v01',
    description = 'Run some sql aggregations on database.',
    start_date = datetime(2023,4,24),
    schedule = '00 05 * * *'
)

def etl():
    @task()
    def run_usd_to_eth():
        adapter_params = {
        }
        load_params = {
            'load_type' : 'usd_to_eth',
            'days' : 5000, ## days as int
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None, ## metric_keys as list or None
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # # load
        ad.load(df)

    def run_metrics():
        adapter_params = {
        }
        load_params = {
            'load_type' : 'metrics', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'days' : 'auto', ## days as int our 'auto
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None, ## metric_keys as list or None
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # # load
        ad.load(df)

    run_usd_to_eth()
    run_metrics()

etl()





