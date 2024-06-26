import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.adapters.rpc_funcs.rpc_sync_checker import sync_check

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'email_on_failure': False,
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_rpc_sync_check',
    description='DAG to check if chain nodes are synchronized',
    tags=['utility', 'hourly',],
    start_date=datetime(2023, 12, 1),
    schedule_interval='35 * * * *'
)

def blockchain_sync_dag():
    @task
    def sync_checker():
        sync_check()

    sync_checker()

sync_dag_instance = blockchain_sync_dag()