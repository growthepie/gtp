from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_raw_imx import AdapterRawImx

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='raw_imx',
    description='Load raw data on withdrawals, deposits, trades, orders_filled, transfers, mints.',
    tags=['raw', 'near-real-time'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/15 * * * *'
)

def etl():
    @task()
    def run_imx():
        adapter_params = {
            'load_types' : ['withdrawals', 'deposits', 'transfers', 'mints'],
            'forced_refresh' : 'no',
        }
       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterRawImx(adapter_params, db_connector)
        # extract & load incremmentally
        ad.extract_raw()

    run_imx()
etl()