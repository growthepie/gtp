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
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_oss_directory',
    description='Loads project data from the OSS Directory API',
    tags=['oli', 'daily'],
    start_date=datetime(2023,6,5),
    schedule='06 00 * * *' # must run before utility_airtable DAG
)

def etl():
    @task()
    def run_oss():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_oso import AdapterOSO

        adapter_params = {
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