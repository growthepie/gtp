from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from eim.adapters.adapter_eth_exported import AdapterEthExported
from eim.adapters.adapter_eth_holders import AdapterEthHolders

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='eim_eth_exported',
    description='Load ETH locked on bridges as well as derivative prices.',
    tags=['eim', 'daily'],
    start_date=datetime(2024,10,30),
    schedule='35 02 * * *'
)

def run():
    @task()
    def run_first_block_of_day():
        adapter_params = {}
        load_params = {
            'load_type' : 'first_block_of_day',
            'days' : 2,
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_bridge_balances():
        adapter_params = {}
        load_params = {
            'load_type' : 'bridge_balances',
            'days' : 2,
            #'entities': ['arbitrum']
            'entities': None
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_conversion_rates():
        adapter_params = {}
        load_params = {
            'load_type' : 'conversion_rates',
            'days' : 2,
            #'assets': ['ETH']
            'assets': None
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_exported_eth_equivalent():
        adapter_params = {}
        load_params = {
            'load_type' : 'eth_equivalent',
            'days' : 2
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_exported_in_usd():
        adapter_params = {}
        load_params = {
            'load_type' : 'eth_equivalent_in_usd',
            'days' : 2
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)


    ## Holder tasks
    @task() ## no dependencies
    def run_get_holders():
        db_connector = DbConnector()

        adapter_params = {}
        load_params = {
            'load_type' : 'get_holders',
        }

        # initialize adapter
        ad = AdapterEthHolders(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df, 'eim_holders')

    @task() 
    def run_offchain_balances():
        db_connector = DbConnector()

        adapter_params = {}
        load_params = {
            'load_type' : 'offchain_balances'
        }
        # initialize adapter
        ad = AdapterEthHolders(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task() ## after run_first_block_of_day
    def run_onchain_balances():
        db_connector = DbConnector()

        adapter_params = {}
        load_params = {
            'load_type' : 'onchain_balances',
            'days' : 2,
        }
        # initialize adapter
        ad = AdapterEthHolders(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task() ## after run_first_block_of_day
    def run_holders_eth_equivalent():
        db_connector = DbConnector()

        adapter_params = {}
        load_params = {
            'load_type' : 'eth_equivalent',
        }

        # initialize adapter
        ad = AdapterEthHolders(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task() ## after run_first_block_of_day
    def run_holders_in_usd():
        db_connector = DbConnector()

        adapter_params = {}
        load_params = {
            'load_type' : 'eth_equivalent_in_usd',
        }

        # initialize adapter
        ad = AdapterEthHolders(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    # No dependencies
    run_get_holders()    

    # Tasks that should only run once but have downstream dependencies
    first_block_of_day = run_first_block_of_day()
    conversion_rates = run_conversion_rates() 

    # Sequence 1
    first_block_of_day >> conversion_rates >> run_bridge_balances() >> run_exported_eth_equivalent() >> run_exported_in_usd()

    # Seuqence 2
    [first_block_of_day, conversion_rates] >> run_onchain_balances() >> run_holders_eth_equivalent() >> run_offchain_balances >> run_holders_in_usd()

run()