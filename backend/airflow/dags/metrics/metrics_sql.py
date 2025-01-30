import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_sql',
    description='Run some sql aggregations on database.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='30 04 * * *'
)

def etl():
    @task()
    def run_metrics_dependent():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'metrics', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'days' : 'auto', ## days as int or 'auto
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None, ## metric_keys as list or None
            'currency_dependent' : True,
            'upsert' : True, ## upsert after each query run
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)
        # # # load
        # ad.load(df)

    @task()
    def run_metrics_independent():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'metrics', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'days' : 'auto', ## days as int or 'auto
            'origin_keys' : None, ## origin_keys as list or None
            'metric_keys' : None, ## metric_keys as list or None
            'currency_dependent' : False,
            'upsert' : True, ## upsert after each query run
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)
        # # # load
        # ad.load(df)

    @task()
    def run_economics(run_metrics:str):
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'economics', ## calculate profit based on rent and fees
            'days' : 5000, ## days as int our 'auto
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

    @task()
    def run_da_metrics(run_economics:str):
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'da_metrics', ## calculate fdv based on total supply and price
            'days' : 30, ## days as int our 'auto
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

    @task()
    def run_fdv(run_da_metrics:str):
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'fdv', ## calculate fdv based on total supply and price
            'days' : 5000, ## days as int our 'auto
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

    @task()
    def run_usd_to_eth(run_fdv:str):
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

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

    @task()
    def run_eth_to_usd(run_usd_to_eth:str):
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        adapter_params = {
        }
        load_params = {
            'load_type' : 'eth_to_usd',
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

    run_eth_to_usd(run_usd_to_eth(run_fdv(run_da_metrics(run_economics(run_metrics_dependent())))))    
    run_metrics_independent()
etl()