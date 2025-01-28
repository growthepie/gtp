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
    description='Run blockspace sql aggregations on database.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='10 02 * * *' ## after coingecko, before sql materialize
)

def etl():
    @task()
    def run_blockspace():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL

        db_connector = DbConnector()

        adapter_params = {
        }

        load_params = {
            'load_type' : 'blockspace', ## usd_to_eth or metrics or blockspace
            'days' : 'auto', ## days as or auto
            'origin_keys' : None, ## origin_keys as list or None
        }

        # initialize adapter
        ad = AdapterSQL(adapter_params, db_connector)

        # extract
        ad.extract(load_params)

    run_blockspace()
etl()