from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_imx import AdapterRawImx


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_raw_imx_v01',
    description = 'Load raw data on withdrawals, deposits, trades, orders_filled, transfers, mints.',
    start_date = datetime(2023,4,24),
    schedule = '30 02 * * *'
)

def etl():
    @task()
    def run_imx():
        adapter_params = {
            'load_types' : ['withdrawals', 'deposits', 'trades', 'orders_filled', 'transfers', 'mints'],
            'forced_refresh' : 'no',
        }
        load_params = {
            'keys' : ['polygon_zkevm_tx'],
            'block_start' : 'auto', ## 'auto' or a block number as int
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterRawImx(adapter_params, db_connector)
        # extract & load incremmentally
        df = ad.extract_raw(load_params)

    run_imx()

etl()
