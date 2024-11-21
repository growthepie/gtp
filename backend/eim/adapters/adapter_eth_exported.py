import pandas as pd
from web3 import Web3
from datetime import datetime
from web3.middleware import geth_poa_middleware

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract
from eim.funcs import get_block_numbers, get_eim_yamls, get_eth_balance, get_erc20_balance_ethereum, call_contract_function

class AdapterEthExported(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ETH exported", adapter_params, db_connector)
        rpc_url = self.db_connector.get_special_use_rpc('ethereum')
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        eim_yamls = get_eim_yamls(['eth_derivatives', 'eth_exported_entities'])
        self.eth_derivatives = eim_yamls[0]
        self.ethereum_token_addresses = self.eth_derivatives.keys()
        self.eth_exported_entities = eim_yamls[1]
        
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        days:int - days of historical data that should be loaded, starting from today.
        load_type:str - the type of data that should be loaded. Supported types are 'first_block_of_day'
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
        
    """
    def extract(self, load_params:dict):
        self.days = load_params['days']
        self.load_type = load_params['load_type']
        
        if self.load_type == 'first_block_of_day':
            df = self.prep_first_block_of_day_data()
        elif self.load_type == 'bridge_balances':
            self.entities = load_params['entities']                
            df = self.get_balances_per_entity(self.entities)
        elif self.load_type == 'conversion_rates':
            self.assets = load_params['assets']
            df = self.get_conversion_rate_per_asset(self.assets)
        elif self.load_type == 'eth_equivalent':
            df = self.get_eth_equivalent_exported()
        elif self.load_type == 'eth_equivalent_in_usd':
            df = self.get_eth_equivalent_in_usd()
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")

        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df:pd.DataFrame):
        tbl_name = 'eim_fact'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    ### Helper functions
    def prep_first_block_of_day_data(self):
        df_main = pd.DataFrame()
        chains = ['ethereum', 'arbitrum', 'optimism', 'base']

        for chain in chains:
            print(f"Processing {chain}")
            rpc_url = self.db_connector.get_special_use_rpc(chain)
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            
            df = get_block_numbers(w3, days=self.days)
            df['origin_key'] = chain
            df['metric_key'] = 'first_block_of_day'

            # rename block column to value
            df.rename(columns={'block':'value'}, inplace=True)
            # drop block_timestamp column
            df.drop(columns=['block_timestamp'], inplace=True)

            df_main = pd.concat([df_main, df])

        ## remove duplicates and set index
        df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df_main
    
    def get_balances_per_entity(self, entities:list=None):
        df_blocknumbers = self.db_connector.get_eim_fact('first_block_of_day', ['ethereum'], days=self.days)
        df_blocknumbers['block'] = df_blocknumbers['value'].astype(int).astype(str)
        df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
        df_main = pd.DataFrame()

        if entities is None:
            entity_list = self.eth_exported_entities.keys()
        else:
            entity_list = entities

        ## iterate through all ethconomies/entities and get balances for each tracked bridge and token
        print(f"Entities to process: {entity_list}")
        for entity in entity_list:
            print(f"Processing {entity}")     

            # iterating over all assets
            for asset_dict in self.eth_exported_entities[entity]['ethereum']:
                asset = list(asset_dict.keys())[0]

                df = df_blocknumbers.copy()
                df['origin_key'] = entity
                df['asset'] = asset.lower()
                #df['value'] = 0
                
                bridge_addresses = asset_dict[asset][0]['address']
                if asset != 'ETH':
                    token_contract = self.eth_derivatives[asset]['ethereum']['contract']
                    token_abi = self.eth_derivatives[asset]['ethereum']['abi']        
                print(f"..processing asset: {asset}")

                # iterating over each date and each contract
                contract_deployed = True
                for i in range(len(df)-1, -1, -1):
                    date = df['date'].iloc[i]
                    block = df['block'].iloc[i]
                    print(f"...retrieving balance for {asset} at block {block} ({date})")

                    balance = 0
                    for address in bridge_addresses:
                        #print(f"....processing bridge_address: {address}")
                        if asset == 'ETH':
                            balance += get_eth_balance(self.w3, address, block)
                        else:
                            bal_new = get_erc20_balance_ethereum(self.w3, token_contract, token_abi, address, block)
                            if bal_new is not None:
                                balance += bal_new
                            else:
                                contract_deployed = False
                                break
            
                    df.loc[i, 'value'] = balance

                    if not contract_deployed:
                        print(f"....contract for {asset} not deployed at block {block} ({date}). Stop processing.")
                        break

                df_main = pd.concat([df_main, df])
        
        # create metric_key column based on concatenated 'eth_exported' and 'asset'
        df_main['metric_key'] = 'eth_exported_' + df_main['asset'].astype(str)
        
        # drop block column
        df_main.drop(columns=['block', 'asset'], inplace=True)

        ## remove 0s, duplicates, set index
        df_main = df_main[df_main['value'] != 0]
        df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df_main = df_main.dropna()
        df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df_main
    
    def get_conversion_rate_per_asset(self, assets:list=None):
        df_blocknumbers = self.db_connector.get_eim_fact('first_block_of_day', ['ethereum'], days=self.days)
        df_blocknumbers['block'] = df_blocknumbers['value'].astype(int).astype(str)
        df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
        df_main = pd.DataFrame()

        if assets is None:
            asset_list = self.ethereum_token_addresses
        else:
            asset_list = assets

        # only works for assets on ethereum L1 as of now!
        for asset in asset_list:
            print(f"..processing asset: {asset}")

            df = df_blocknumbers.copy()
            df['metric_key'] = 'price_eth'
            df['asset'] = asset.lower()

            start_date = None
            fixed = self.eth_derivatives[asset]['fixed'] if 'fixed' in self.eth_derivatives[asset] else False

            if asset not in ['ETH', 'nativeStakedETH']:
                if 'price_contract' in self.eth_derivatives[asset]['ethereum']:
                    token_contract = self.eth_derivatives[asset]['ethereum']['price_contract']
                    token_abi = self.eth_derivatives[asset]['ethereum']['price_abi']  
                else:
                    token_contract = self.eth_derivatives[asset]['ethereum']['contract']
                    token_abi = self.eth_derivatives[asset]['ethereum']['abi']  

                function_name = self.eth_derivatives[asset]['ethereum']['function'] if 'function' in self.eth_derivatives[asset]['ethereum'] else None
                args = tuple(self.eth_derivatives[asset]['ethereum']['args']) if 'args' in self.eth_derivatives[asset]['ethereum'] else ()                
                start_date = self.eth_derivatives[asset]['ethereum']['start_date'] if 'start_date' in self.eth_derivatives[asset]['ethereum'] else None

            contract_deployed = True
            # iterate through each row from reverse until we get price = 0
            for i in range(len(df)-1, -1, -1):
                date = df['date'].iloc[i]
                block = df['block'].iloc[i]

                if start_date and date < datetime.strptime(start_date, '%Y-%m-%d').date():
                    break

                if fixed:
                    price = 1
                elif asset == 'ezETH':
                    ## custom logic for ezETH
                    main_contract = self.eth_derivatives[asset]['ethereum']['contract']
                    main_abi = self.eth_derivatives[asset]['ethereum']['abi']
                    total_supply_ezETH = call_contract_function(self.w3, main_contract, main_abi, 'totalSupply', at_block=block)
                    total_eth = call_contract_function(self.w3, token_contract, token_abi, 'calculateTVLs', at_block=block)[2] 
                    if total_supply_ezETH and total_eth:
                        price = total_eth / total_supply_ezETH
                    else:
                        contract_deployed = False
                elif asset == 'rsETH':
                    ## custom logic for rsETH
                    price = call_contract_function(self.w3, token_contract, token_abi, function_name, *args, at_block=block)
                    if price is None:
                        contract_deployed = False
                    else:
                        price = 10**18 / price
                else:
                    price = call_contract_function(self.w3, token_contract, token_abi, function_name, *args, at_block=block)
                    if price is None:
                        contract_deployed = False
                    else:
                        price = price / 10**18

                if not contract_deployed:
                    print(f"....contract for {asset} not deployed at block {block} ({date}). Stop processing.")
                    break
                
                df.loc[i, 'value'] = price
                print(f"..date: {date}, block: {block}, price: {price}")

            df_main = pd.concat([df_main, df])

        # create metric_key column based on concatenated 'eth_exported' and 'asset'
        df_main['origin_key'] = 'asset_' + df_main['asset'].astype(str)

        # drop block column
        df_main.drop(columns=['block', 'asset'], inplace=True)

        ## remove 0s, nulls, duplicates, set index
        df_main = df_main[df_main['value'] != 0]
        df_main = df_main.dropna()
        df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df_main
    
    def get_eth_equivalent_exported(self):
        df_eth_exported = self.db_connector.get_eth_exported(self.days)
        df_price_eth = self.db_connector.get_eim_fact('price_eth', days=self.days)
        df_price_eth['asset'] = df_price_eth['origin_key'].str.split('_').str[1]

        ## merge df_eth_exported and df_price_eth based on date and asset
        df = pd.merge(df_eth_exported, df_price_eth, on=['date', 'asset'], how='inner')

        ## divide value_x by value_y
        df['value'] = df['value_x'] / df['value_y']

        ## only keep columns origin_key_x, date, value
        df = df[['origin_key_x', 'date', 'value']]
        df.columns = ['origin_key', 'date', 'value']

        df['metric_key'] = 'eth_equivalent_exported_eth'

        ## group by origin_key, date, metric_key and sum value
        df = df.groupby(['origin_key', 'date', 'metric_key']).sum()
        return df
    
    def get_eth_equivalent_in_usd(self):
        df = self.db_connector.get_values_in_usd_eim(['eth_equivalent_exported_eth'], self.days)
        df = df[df['value'] != 0]
        df = df.dropna()
        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df
        
