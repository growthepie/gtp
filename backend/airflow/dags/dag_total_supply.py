from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

# import .env
import os
from dotenv import load_dotenv
load_dotenv() 

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")
import pandas as pd
from web3 import Web3
import time

# import other packages
from airflow.decorators import dag, task
from src.db_connector import DbConnector
from src.adapters.adapter_total_supply import AdapterTotalSupply

### DAG
default_args = {
    'owner' : 'lorenz',
    'retries' : 2,
    'email' : ['lorenz@growthepie.xyz', 'manish@growthepie.xyz', 'matthias@growthepie.xyz'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=15)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_totalSupply',
    description = 'Get KPI totalSupply for tokens of L2 chains',
    start_date = datetime(2024,2,20),
    schedule = '50 02 * * *'
)


def etl():
    @task()
    def load_data():
        adapter_params = {
            'etherscan_api' : os.getenv("ETHERSCAN_API")
        }
        load_params = { 
            'days' : 'auto', ## days as int our 'auto'
            'origin_keys' : None, ## origin_keys as list or None
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterTotalSupply(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    load_data()

etl()
