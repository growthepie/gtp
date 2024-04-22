from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.adapters.adapter_starknet_proof import AdapterStarknetProof

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_starknet_proof',
    description='Load Starknets Proof Costs.',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,4,21),
    schedule='30 03 * * *'
)

def etl():
    @task()
    def run_tvl():
        adapter_params = {
        }
        load_params = {
            'days' : 7,
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterStarknetProof(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_tvl()
etl()