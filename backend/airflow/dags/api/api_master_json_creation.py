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
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_master_json_creation',
    description='Create master json file.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule=None,
    params={"api_version": "v1"}
)

def json_creation():
    @task()
    def run_create_master(**kwargs):
        import os
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        # Get the api_version from DAG parameters
        api_version = kwargs['params'].get('api_version', 'v1')

        db_connector = DbConnector()

        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        df = json_creator.get_all_data()
        json_creator.create_master_json(df)

    # Main
    run_create_master()    
   
json_creation()