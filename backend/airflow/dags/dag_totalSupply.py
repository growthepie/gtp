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
from src.chain_config import adapter_mapping
from src.misc.helper_functions import get_df_kpis_with_dates, get_missing_days_kpis, api_get_call, upsert_to_kpis


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
    schedule = '00 05 * * *'
)


def etl():
    
    @task()
    def load_data():
        db = DbConnector()
        coins = [x for x in adapter_mapping if x.token_address is not None]
        for coin in coins:
            try:
                # get the missing days
                days = get_missing_days_kpis(db, 'total_supply', coin.origin_key)
                max_days = (datetime.now() - datetime.strptime(coin.token_deployment_date, '%Y-%m-%d')).days
                if days > max_days:
                    days = max_days

                # build the dataframe with block heights
                df = get_df_kpis_with_dates(days)
                df['origin_key'] = coin.origin_key
                df['metric_key'] = 'total_supply'
                if coin.token_deployment_origin_key == 'ethereum':
                    for index, row in df.iterrows():
                        t = int((row['date'] + timedelta(hours=23, minutes=59, seconds=59)).timestamp())
                        df.loc[index, 'block_number'] = api_get_call(f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={t}&closest=before&apikey={os.getenv('ETHERSCAN_API_KEY')}")['result']
                        time.sleep(0.25)
                    rpc = [x for x in adapter_mapping if x.origin_key == 'ethereum'][0].rpc_url
                else:
                    df2 = db.get_total_supply_blocks(coin.origin_key, days)
                    df2['date'] = pd.to_datetime(df2['date'])
                    df = df.merge(df2, on='date', how='left')
                    rpc = coin.rpc_url

                # load in the contract
                w3 = Web3(Web3.HTTPProvider(rpc))
                contract = w3.eth.contract(address=coin.token_address, abi=coin.token_abi)
                print(f'Connected to {coin.token_deployment_origin_key} at {rpc}')

                # get the total supply for each block
                decimals = contract.functions.decimals().call()
                df['block_number'] = df['block_number'].astype(int)
                for index, row in df.iterrows():
                    totalsupply = contract.functions.totalSupply().call(block_identifier=row['block_number'])/10**decimals
                    df.loc[index, 'value'] = totalsupply
                    time.sleep(0.25)
                
                # upsert the data
                df = df.drop(columns=['block_number'])
                df = df.set_index(['origin_key', 'metric_key', 'date'])
                upsert_to_kpis(df, db)
            except:
                print(f"Error with {coin.origin_key}")
                continue

    load_data()

etl()

