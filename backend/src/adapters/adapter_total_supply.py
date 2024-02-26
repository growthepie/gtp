import time
import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3

from src.adapters.abstract_adapters import AbstractAdapter
from src.chain_config import adapter_mapping
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_missing_days_kpis, upsert_to_kpis, get_df_kpis_with_dates
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterTotalSupply(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Total supply", adapter_params, db_connector)
        self.api_key = adapter_params['etherscan_api']
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        ## Set variables
        origin_keys = load_params['origin_keys']
        days = load_params['days']

        projects = [x for x in adapter_mapping if x.token_address is not None]
        
        ## Prepare projects to load (can be a subset of all projects)
        check_projects_to_load(projects, origin_keys)
        projects_to_load = return_projects_to_load(projects, origin_keys)

        ## Load data
        df = self.extract_data(
            projects_to_load=projects_to_load,
            days_load = days
            )

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    def extract_data(self, projects_to_load, days_load):
        dfMain = pd.DataFrame()
        for coin in projects_to_load:            
            try:
                # get the missing days
                if days_load == 'auto':
                    days = get_missing_days_kpis(self.db_connector, 'total_supply', coin.origin_key)
                else:
                    days = int(days_load)

                print(f"...loading {coin.origin_key} for {days} days.")

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
                        df.loc[index, 'block_number'] = api_get_call(f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={t}&closest=before&apikey={self.api_key}")['result']
                        time.sleep(0.25)
                    rpc = [x for x in adapter_mapping if x.origin_key == 'ethereum'][0].rpc_url
                else:
                    df2 = self.db_connector.get_total_supply_blocks(coin.origin_key, days)
                    df2['date'] = pd.to_datetime(df2['date'])
                    df = df.merge(df2, on='date', how='left')
                    rpc = coin.rpc_url

                # load in the contract
                w3 = Web3(Web3.HTTPProvider(rpc))
                contract = w3.eth.contract(address=coin.token_address, abi=coin.token_abi)
                print(f'...connected to {coin.token_deployment_origin_key} at {rpc}')

                # get the total supply for each block
                decimals = contract.functions.decimals().call()
                df['block_number'] = df['block_number'].astype(int)
                for index, row in df.iterrows():
                    totalsupply = contract.functions.totalSupply().call(block_identifier=row['block_number'])/10**decimals
                    df.loc[index, 'value'] = totalsupply
                    time.sleep(0.25)
                
                # drop the block number
                df = df.drop(columns=['block_number'])

                print(f"Loaded {coin.origin_key} for {days} days. Total of {df.shape[0]} rows.")

                dfMain = pd.concat([dfMain,df])
            except:
                print(f"Error with {coin.origin_key}")
                continue
        
        #print(dfMain.to_markdown())
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain