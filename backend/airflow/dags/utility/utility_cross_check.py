from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_cross_check import AdapterCrossCheck

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='utility_cross_check',
    description='Load txcount data from explorers and check against our database. Send discord message if there is a discrepancy.',
    tags=['utility', 'daily'],
    start_date=datetime(2023,12,9),
    schedule='30 06 * * *'
)

def etl():
    @task()
    def run_explorers():
        adapter_params = {
        }

        load_params = {
            'origin_keys' : None,
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCrossCheck(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

        ## cross-check and send Discord message
        ad.cross_check()
    
    run_explorers()
etl()