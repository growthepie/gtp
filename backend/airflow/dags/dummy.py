from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")
from airflow.decorators import dag, task 

from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(seconds=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='dummy',
    description='This is a dummy DAG that is supposed to fail.',
    tags=['dummy'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/15 * * * *'
)

def etl():
    @task()
    def run_dummy_task():
        raise Exception("This is a dummy task that is supposed to fail.")
    
    run_dummy_task()
etl()