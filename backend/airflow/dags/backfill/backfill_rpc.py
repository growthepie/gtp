import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.adapters.funcs_backfill import backfiller_task

chain_settings = {
    'gitcoin_pgn': {'threads': 10, 'batch_size': 50},
    'linea': {'threads': 5, 'batch_size': 10},
    'zora': {'threads': 15, 'batch_size': 50},
    'scroll': {'threads': 3, 'batch_size': 20},
    'mantle': {'threads': 15, 'batch_size': 50},
    'base': {'threads': 3, 'batch_size': 50},
    'optimism': {'threads': 3, 'batch_size': 50},
    'metis': {'threads': 1, 'batch_size': 50},
    'polygon_zkevm': {'threads': 5, 'batch_size': 150},
}

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
        'email_on_failure': True,
        'retry_delay': timedelta(minutes=5)
    },
    dag_id='backfill_rpc',
    description='DAG for backfilling missing blockchain data',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 12, 1),
    schedule='20 11 * * *'
)

def backfiller_dag():
    chains = ['gitcoin_pgn', 'linea', 'zora', 'scroll', 'mantle', 'base', 'polygon_zkevm', 'optimism', 'metis']

    for chain in chains:
        @task(task_id=f'backfill_{chain}')
        def run_backfill_task(chain_name, start_date, end_date, threads, batch_size):
            try:
                backfiller_task(chain_name, start_date, end_date, threads, batch_size)
            except Exception as e:
                print(f"An error occurred in backfiller_task for {chain_name}: {e}")
                raise e

        try:
            # Calculate the date range for the backfill
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') # 7 days ago
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') # Yesterday

            # Get threads and batch size from the chain_settings
            threads = chain_settings[chain]['threads']
            batch_size = chain_settings[chain]['batch_size']

            # Invoke the backfill task for each chain
            run_backfill_task(chain, start_date, end_date, threads, batch_size)
        except Exception as e:
            print(f"An error occurred while setting up backfill for {chain}: {e}")
            raise e

backfiller_dag_instance = backfiller_dag()