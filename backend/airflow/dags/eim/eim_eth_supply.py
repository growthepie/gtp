import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

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
    schedule='38 03 * * *'
)

def run():
    @task()
    def run_extract_eth_supply():
        from src.db_connector import DbConnector
        from eim.adapters.adapter_eth_supply import AdapterEthSupply

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
        from src.db_connector import DbConnector
        from eim.adapters.adapter_eth_supply import AdapterEthSupply

        adapter_params = {}
        load_params = {
            'load_type' : 'supply_in_usd',
            'days' : 5,
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_issuance_rate(x):
        from src.db_connector import DbConnector
        from eim.adapters.adapter_eth_supply import AdapterEthSupply
        
        adapter_params = {}
        load_params = {
            'load_type' : 'issuance_rate',
            'days' : 5
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    run_issuance_rate(run_supply_in_usd(run_extract_eth_supply()))

run()