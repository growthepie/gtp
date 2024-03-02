from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_sql import AdapterSQL

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='utility_sql_materialize',
    description='Aggregate materialized views on database',
    tags=['utility', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='00 02 * * *'
)

def etl():
    @task()
    def run_unique_senders():
        adapter_params = {
        }
        load_params = {
            'load_type' : 'active_addresses_agg', ## load metrics such as imx txcount, daa, fees paid and user_base metric
            'days' : 7, ## days as int or 'auto
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # # load
        ad.load(df)

    run_unique_senders()
etl()





