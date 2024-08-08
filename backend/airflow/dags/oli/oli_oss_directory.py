from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_oso import AdapterOSO

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_oss_directory',
    description='Loads project data from the OSS Directory API',
    tags=['oli', 'daily'],
    start_date=datetime(2023,6,5),
    schedule='05 02 * * *'
)

def etl():
    @task()
    def run_oss():
        adapter_params = {
            'github_token' : os.getenv("GITHUB_TOKEN"), # add GITHUB_TOKEN to .env
            'webhook' : os.getenv('DISCORD_CONTRACTS')
        }
        load_params = {
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterOSO(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

        ad.check_inactive_projects()
    
    run_oss()

etl()