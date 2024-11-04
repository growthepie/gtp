from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from eim.adapters.adapter_eth_supply import AdapterEthSupply

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='eim_eth_supply',
    description='Load ETH supply data via ultrasound.money API.',
    tags=['eim', 'daily'],
    start_date=datetime(2024,10,30),
    schedule='38 02 * * *'
)

def run():
    @task()
    def run_extract_eth_supply():
        adapter_params = {}
        load_params = {
            'load_type' : 'extract_eth_supply',
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_supply_in_usd(x):
        adapter_params = {}
        load_params = {
            'load_type' : 'supply_in_usd',
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    run_supply_in_usd(run_extract_eth_supply())

run()