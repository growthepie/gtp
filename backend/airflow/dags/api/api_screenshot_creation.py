from datetime import datetime,timedelta
import getpass
import os
import sys
import dotenv
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")
dotenv.load_dotenv()

from src.api.screenshots_to_s3 import run_screenshots
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_screenshot_creation',
    description='Create and store screenshots in s3 bucket',
    tags=['api', 'daily'],
    start_date = datetime(2023,4,24),
    schedule='00 07 * * *'
)

def etl():
    @task()
    def run_screenshots_task():
        run_screenshots(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), 'v1', sys_user)
    
    run_screenshots_task()
etl()