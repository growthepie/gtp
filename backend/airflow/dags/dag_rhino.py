import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.adapters.adapter_rhino import AdapterRhino
from src.db_connector import DbConnector
from src.adapters.adapter_utils import *

default_args = {
    'owner': 'nader',
    'retries': 2,
    'email': ['nader@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_rhino',
    description='Load raw tx data from Rhino',
    start_date=datetime(2023, 9, 1),
    schedule = '35 03 * * *'
)

def adapter_rhino_tx_loader():
    @task()
    def run_rhino_tx_loader():
        adapter_params = {
            'chain': 'rhino',
            'json_endpoint': os.getenv("RHINO_JSON"),
        }

        # Initialize DbConnector
        db_connector = DbConnector()

        # Initialize AdapterRhino
        adapter = AdapterRhino(adapter_params, db_connector)

        try:
            adapter.extract_raw()
            print("Successfully loaded transaction data for Rhino.")
        except Exception as e:
            print(f"Failed to load transaction data: {e}")
            raise

    run_rhino_tx_loader()

adapter_rhino_tx_loader()
