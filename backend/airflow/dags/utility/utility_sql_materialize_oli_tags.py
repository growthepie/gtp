from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_sql import AdapterSQL

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_sql_materialize_oli_tags',
    description='Aggregate materialized views on database',
    tags=['utility', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='50 02 * * *'
)

def dag_run():
    @task()
    def run_refresh_materialized_view():
        db_connector = DbConnector()
        db_connector.refresh_materialized_view('vw_oli_labels_materialized')

    run_refresh_materialized_view()
dag_run()





