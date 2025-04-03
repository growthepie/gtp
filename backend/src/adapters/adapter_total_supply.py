import time
import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_missing_days_kpis, upsert_to_kpis, get_df_kpis_with_dates
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterTotalSupply(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Total supply", adapter_params, db_connector)
        self.main_conf = get_main_config()
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

        projects = [chain for chain in self.main_conf if chain.cs_token_address is not None] ## NEED to add these fields
        
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

                max_days = (datetime.now() - datetime.strptime(coin.cs_deployment_date , '%Y-%m-%d')).days
                if days > max_days:
                    days = max_days

                # build the dataframe with block heights
                df = get_df_kpis_with_dates(days)
                df['origin_key'] = coin.origin_key
                df['metric_key'] = 'total_supply'
                if coin.cs_deployment_origin_key == 'ethereum':
                    block_df = self.db_connector.get_data_from_table(
                        "fact_kpis",
                        filters={
                            "metric_key": "first_block_of_day",
                            "origin_key": "ethereum"
                        },
                        days=days
                    )

                    if block_df.empty:
                        raise ValueError("Missing block data in fact_kpis for Ethereum")

                    block_df = block_df.reset_index()
                    block_df['block_number'] = block_df['value'].astype(int)
                    block_df = block_df[['date', 'block_number']]
                    
                    # Merge with df
                    df = df.merge(block_df, on='date', how='left')

                    rpc = self.db_connector.get_special_use_rpc('ethereum')
                elif coin.origin_key == 'zksync_era':
                    df['value'] = 21000000000
                    dfMain = pd.concat([dfMain,df])
                    continue
                elif coin.origin_key == 'celo':
                    df['value'] = 1000000000
                    dfMain = pd.concat([dfMain,df])
                    continue
                else:
                    df2 = self.db_connector.get_total_supply_blocks(coin.origin_key, days)
                    df2['date'] = pd.to_datetime(df2['date'])
                    df = df.merge(df2, on='date', how='left')
                    rpc = self.db_connector.get_special_use_rpc(coin.origin_key)

                # Defined a basic ABI for totalSupply and decimals
                token_abi = [
                    {
                        "constant": True,
                        "inputs": [],
                        "name": "totalSupply",
                        "outputs": [{"name": "", "type": "uint256"}],
                        "type": "function"
                    },
                    {
                        "constant": True,
                        "inputs": [],
                        "name": "decimals",
                        "outputs": [{"name": "", "type": "uint8"}],
                        "type": "function"
                    }
                ]

                # load the contract with the basic ABI
                w3 = Web3(Web3.HTTPProvider(rpc))
                token_address = coin.cs_token_address if Web3.is_checksum_address(coin.cs_token_address) else Web3.to_checksum_address(coin.cs_token_address)
                contract = w3.eth.contract(address=token_address, abi=token_abi)
                print(f'...connected to {coin.cs_deployment_origin_key} at {rpc}')
                time.sleep(1)

                # get the total supply for each block
                decimals = contract.functions.decimals().call()
                time.sleep(1)

                df['block_number'] = df['block_number'].astype(int)
                for index, row in df.iterrows():
                    retry_counter = 0
                    while True:
                        try:
                            totalsupply = contract.functions.totalSupply().call(block_identifier=row['block_number'])/10**decimals
                            break
                        except Exception as e:
                            if retry_counter > 5:
                                print(f"Error with {coin.origin_key}: {e}")
                                totalsupply = None
                                raise e
                            print(f"..{retry_counter} - retrying {coin.origin_key} for block {row['block_number']}: {e}")
                            retry_counter += 1
                            time.sleep(3)

                    df.loc[index, 'value'] = totalsupply
                    time.sleep(1)
                
                # drop the block number
                df = df.drop(columns=['block_number'])

                print(f"Loaded {coin.origin_key} for {days} days. Total of {df.shape[0]} rows.")

                dfMain = pd.concat([dfMain,df])
            except Exception as e:
                print(f"Error with {coin.origin_key}: {e}")
                #raise e
                continue
        
        #print(dfMain.to_markdown())
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain