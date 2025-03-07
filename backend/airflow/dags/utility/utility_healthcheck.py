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
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_dummy',
    description='This is a dummy DAG that is supposed to fail.',
    tags=['utility'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/10 * * * *'
)

def healthcheck():
    @task()
    def run_healthcheck_ping():
        try:
            import os
            import requests
            import dotenv
            dotenv.load_dotenv()

            # Healthchecks.io URL
            HEALTHCHECKS_URL = os.getenv("HEALTHCHECKS_URL")
            response = requests.get(HEALTHCHECKS_URL, timeout=5)
            response.raise_for_status()  # Raises an error if the request fails
            print("✅ Heartbeat sent successfully!")
        except requests.RequestException as e:
            print(f"❌ Failed to send heartbeat: {e}")
    
    run_healthcheck_ping()
healthcheck()