import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.adapters.loopring_tokens import fetch_and_insert_tokens

default_args = {
    'owner': 'nader',
    'retries': 2,
    'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_loopring_tokens',
    description='Load fee tokens from Loopring',
    start_date=datetime(2023, 9, 1),
    schedule_interval='40 */3 * * *'
)

def loopring_tokens():
    @task()
    def fetch_tokens():
        fetch_and_insert_tokens()
        
    fetch_tokens()
    
loopring_tokens()


