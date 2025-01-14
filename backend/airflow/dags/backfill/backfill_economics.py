import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='backfill_economics',
    description='Backfill economics and da values for new entries or changes in gtp_dna economics mapping',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='30 09 * * *'
)


@task(execution_timeout=timedelta(minutes=30))
def backfill():
    import os
    from src.db_connector import DbConnector
    
    adapter_params = {
        'chain': 'celestia',
        'rpc_url': os.getenv("TIA_RPC"),
    }

    # Initialize DbConnector
    db_connector = DbConnector()


backfill()