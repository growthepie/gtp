from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from src.api.screenshots_to_s3 import run_screenshots
from airflow.decorators import dag, task 
import os

default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com', 'mike@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_screenshot_loader',
    description = 'Create and store screenshots in s3 bucket',
    start_date = datetime(2023,4,24),
    schedule = '00 06 * * *'
)

def etl():
    @task()
    def run_screenshots_task():
        run_screenshots(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), 'v1')
    
    run_screenshots_task()
etl()

