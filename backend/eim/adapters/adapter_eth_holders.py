import pandas as pd
from web3 import Web3
from datetime import datetime
import getpass
sys_user = getpass.getuser()


from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract
from eim.funcs import read_yaml_file, get_eth_balance, get_erc20_balance_ethereum, call_contract_function

class AdapterEthHolders(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ETH holders", adapter_params, db_connector)
        if sys_user == 'ubuntu':
            self.eth_derivatives = read_yaml_file(f'/home/{sys_user}/gtp/backend/eim/eth_derivatives.yml')
            self.eth_holders = read_yaml_file(f'/home/{sys_user}/gtp/backend/eim/eth_holders.yml')
        else:
            self.eth_derivatives = read_yaml_file('eim/eth_derivatives.yml')
            self.eth_holders = read_yaml_file('eim/eth_holders.yml')

        self.assets = ['ETH', 'stETH', 'wETH', 'wstETH', 'mETH']
        
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        days:int - days of historical data that should be loaded, starting from today.
        load_type:str - the type of data that should be loaded. Supported types are 'first_block_of_day'
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
        
    """
    def extract(self, load_params:dict):
        self.days = load_params.get('days', 1)
        self.load_type = load_params['load_type']
        self.holders = load_params.get('holders', None)  

        if self.load_type == 'get_holders':
            df = self.get_holders()
        elif self.load_type == 'onchain_balances':                        
            df = self.get_onchain_balances(self.holders)
        elif self.load_type == 'offchain_balances':
            df = self.get_offchain_balances(self.holders)
        elif self.load_type == 'eth_equivalent':
            df = self.get_eth_equivalent()
        elif self.load_type == 'eth_equivalent_in_usd':
            df = self.get_eth_equivalent_in_usd()
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")

        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df:pd.DataFrame, table_name:str=None):
        if table_name is None:
            tbl_name = 'eim_holders_balance'
        else:
            tbl_name = table_name

        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    ### Helper functions    
    def get_holders(self, holders:list=None):
        if holders is None:
            all_holders = {k:v for k,v in self.eth_holders.items()}
            holders_list = all_holders.keys()
        else:
            holders_list = holders

        ## create df from eth_holders with holder_key, name, type
        df = pd.DataFrame()
        for holder in holders_list:
            print(f"Processing {holder}")
            holder_key = holder
            holder_name = self.eth_holders[holder]['name']
            holder_type = self.eth_holders[holder]['type']
            holding_type = self.eth_holders[holder]['holding_type']
            df = pd.concat([df, pd.DataFrame({'holder_key': [holder_key], 'name': [holder_name], 'type': [holder_type], 'holding_type': [holding_type]})])

        df.set_index('holder_key', inplace=True)
        return df

    def get_onchain_balances(self, holders:list=None):
        df_main = pd.DataFrame()
        df_blocknumbers = self.db_connector.get_eim_fact('first_block_of_day', days=self.days)
        df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
        df_blocknumbers.drop(columns=['value'], inplace=True) 

        if holders is None:
            onchain_holders = {k:v for k,v in self.eth_holders.items() if 'onchain' in v['holding_type']}
            holders_list = onchain_holders.keys()
        else:
            holders_list = holders

        ## iterate through all holders and get balances for each asset and address
        print(f"Entities to process: {holders_list}")
        for holder in holders_list:
            print(f"Processing {holder}")
            # iterating over all assets

            for chain in self.eth_holders[holder]['chains']:                    
                rpc_url = self.db_connector.get_special_use_rpc(chain)
                w3 = Web3(Web3.HTTPProvider(rpc_url))        
                print(f"..processing chain: {chain}. Connecting to {rpc_url}")

                for asset in self.assets:
                    print(f"..processing asset: {asset}")
                    new_df_entries = []
                    df = df_blocknumbers[df_blocknumbers['origin_key'] == chain].copy()
                    df.drop(columns=['origin_key'], inplace=True) 
                    df['holder_key'] = holder
                    df['asset'] = asset.lower()
                    
                    if asset != 'ETH':
                        if chain == 'ethereum':
                            token_contract = self.eth_derivatives[asset]['ethereum']['contract']
                            token_abi = self.eth_derivatives[asset]['ethereum']['abi']  
                        else:
                            print(f"....asset {asset} not YET supported on chain {chain}.")
                            ## TODO: add non ETH support for other chains (add contracts to eth_derivatives.yml)
                            continue

                    # iterating over each date and each contract
                    contract_deployed = True
                    for i in range(len(df)-1, -1, -1):
                        date = df['date'].iloc[i]
                        block = df['block'].iloc[i]
                        balance = 0
                        print(f"...retrieving balance for {asset} at block {block} ({date})")    

                        for address_entry in self.eth_holders[holder]['chains'][chain]:
                            address = address_entry['address']                
                            
                            print(f"....processing address: {address}")
                            if asset == 'ETH':
                                balance += get_eth_balance(w3, address, int(block))
                            else:
                                bal_new = get_erc20_balance_ethereum(w3, token_contract, token_abi, address, int(block))
                                if bal_new is not None:
                                    balance += bal_new
                                else:
                                    contract_deployed = False
                                    break
                    
                        #df.loc[i, 'value'] = balance
                        new_df_entries.append({'holder_key': holder, 'asset': asset.lower(), 'date': date, 'value': balance})

                        if not contract_deployed:
                            print(f"....contract for {asset} not deployed at block {block} ({date}). Stop processing.")
                            break

                    if len(new_df_entries) > 0:
                        df_new = pd.DataFrame(new_df_entries)
                        df_main = pd.concat([df_main, df_new])

        # create metric_key column based on concatenated 'eth_exported' and 'asset'
        df_main['metric_key'] = 'balance_' + df_main['asset'].astype(str)

        # drop block column
        df_main.drop(columns=['asset'], inplace=True)

        ## remove 0s, duplicates, set index
        df_main = df_main[df_main['value'] != 0]
        df_main.drop_duplicates(subset=['metric_key', 'holder_key', 'date'], inplace=True)
        df_main.set_index(['metric_key', 'holder_key', 'date'], inplace=True)
        return df_main
    
    def get_offchain_balances(self, holders:list=None):
        if holders is None:
            onchain_holders = {k:v for k,v in self.eth_holders.items() if 'offchain' in v['holding_type']}
            holders_list = onchain_holders.keys()
        else:
            holders_list = holders

        ## iterate through all holders and get balances for each asset and address
        print(f"Entities to process: {holders_list}")
        new_df_entries = []
        for holder in holders_list:
            print(f"Processing {holder}")
            
            # iterating over all transactions
            balance = 0
            for tx in self.eth_holders[holder]['transactions']:
                print(f"..processing tx: {tx['tx_id']}")
                if tx['tx_type'] == 'buy':
                    balance += tx['amount_eth']
                elif tx['tx_type'] == 'sell':
                    balance -= tx['amount_eth']
                else:
                    raise ValueError(f"tx_type {tx['tx_type']} not supported for this adapter")
                
            new_df_entries.append({'holder_key': holder, 'date': tx['timestamp'], 'value': balance})

        if len(new_df_entries) > 0:
            df = pd.DataFrame(new_df_entries)

        # create metric_key column based on concatenated 'eth_exported' and 'asset'
        df['metric_key'] = 'eth_equivalent_balance_eth'

        ## remove 0s, duplicates, set index
        df = df[df['value'] != 0]
        df.drop_duplicates(subset=['metric_key', 'holder_key', 'date'], inplace=True)
        df.set_index(['metric_key', 'holder_key', 'date'], inplace=True)
        return df

    def get_eth_equivalent(self):
        df_holders_balances = self.db_connector.get_holders_raw_balances(self.days)
        df_price_eth = self.db_connector.get_eim_fact('price_eth', days=self.days)
        df_price_eth['asset'] = df_price_eth['origin_key'].str.split('_').str[1]

        ## merge df_eth_exported and df_price_eth based on date and asset
        df = pd.merge(df_holders_balances, df_price_eth, on=['date', 'asset'], how='inner')

        ## divide value_x by value_y
        df['value'] = df['value_x'] / df['value_y']

        df = df[['holder_key', 'date', 'value']]

        df['metric_key'] = 'eth_equivalent_balance_eth'

        ## group by origin_key, date, metric_key and sum value
        df = df.groupby(['holder_key', 'date', 'metric_key']).sum().reset_index()
        df.set_index(['metric_key', 'holder_key', 'date'], inplace=True)
        return df
    
    def get_eth_equivalent_in_usd(self):
        df = self.db_connector.get_values_in_usd_eim_holders(['eth_equivalent_balance_eth'], self.days)
        df = df[df['value'] != 0]
        df = df.dropna()
        df.drop_duplicates(subset=['metric_key', 'holder_key', 'date'], inplace=True)
        df.set_index(['metric_key', 'holder_key', 'date'], inplace=True)
        return df