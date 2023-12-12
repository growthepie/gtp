import getpass
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.adapters.adapter_utils import *
from adapter_gtp_backfill_task import backfiller_task

sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

chain_settings = {
    'gitcoin_pgn': {'threads': 15, 'batch_size': 250},
    'linea': {'threads': 4, 'batch_size': 200},
    'zora': {'threads': 15, 'batch_size': 250},
    'scroll': {'threads': 2, 'batch_size': 200},
    'mantle': {'threads': 7, 'batch_size': 150},
    'base': {'threads': 3, 'batch_size': 100}
}

default_args = {
    'owner': 'nader',
    'retries': 2,
    'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='backfiller_dag',
    description='DAG for backfilling missing blockchain data',
    start_date=datetime(2023, 12, 1),
    schedule_interval=timedelta(days=1)
)
def backfiller_dag():
    chains = ['gitcoin_pgn', 'linea', 'zora', 'scroll', 'mantle', 'base']

    for chain in chains:
        @task(task_id=f'backfill_{chain}')
        def run_backfill_task(chain_name, start_date, end_date, threads, batch_size):
            try:
                backfiller_task(chain_name, start_date, end_date, threads, batch_size)
            except Exception as e:
                print(f"An error occurred in backfiller_task for {chain_name}: {e}")

        try:
            # Calculate the date range for the backfill
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d') # 3 days ago
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') # Yesterday

            # Get threads and batch size from the chain_settings
            threads = chain_settings[chain]['threads']
            batch_size = chain_settings[chain]['batch_size']

            # Invoke the backfill task for each chain
            run_backfill_task(chain, start_date, end_date, threads, batch_size)
        except Exception as e:
            print(f"An error occurred while setting up backfill for {chain}: {e}")

backfiller_dag_instance = backfiller_dag()
