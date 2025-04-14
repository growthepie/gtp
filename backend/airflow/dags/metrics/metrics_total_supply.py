import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=15),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_total_supply',
    description='Get KPI totalSupply for tokens of L2 chains',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,2,20),
    schedule='20 02 * * *'
)

def etl():
    @task()
    def load_data():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_total_supply import AdapterTotalSupply
        
        adapter_params = {}
        load_params = { 
            'days' : 'auto', ## days as int our 'auto'
            'origin_keys' : None, ## origin_keys as list or None
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterTotalSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    load_data()
etl()