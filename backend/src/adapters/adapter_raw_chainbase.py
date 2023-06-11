import time
import json
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.queries.chainbase_queries import chainbase_raws 
from src.misc.helper_functions import print_init, dataframe_to_s3, api_post_call

class AdapterChainbaseRaw(AbstractAdapterRaw):
    """
    adapter_params require the following fields:
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ChainbaseRaw", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']

        self.url = "https://api.chainbase.online/v1/dw/query"
        self.headers = {
            "x-api-key": self.api_key,
            "content-type": "application/json"
        }

        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        keys:list - the name of the table keys to load the data into
        block_start:int - the block where to start loading the data from. Can be set to 'auto'
    """
    def extract_raw(self, load_params:dict):
        ## Set variables
        self.keys = load_params['keys']
        self.block_start = load_params['block_start']

        self.queries_to_load = [x for x in chainbase_raws if x.key in self.keys]

        ## Trigger queries
        self.trigger_check_extract_queries(self.queries_to_load, self.block_start)
        print(f"FINISHED loading raw data for {self.keys}.")

    ## ----------------- Helper functions --------------------

    def trigger_check_extract_queries(self, queries_to_load, block_start):
        for query in queries_to_load:  
            print(f"START loading raw data for {query.key}.")          
            dfMain = pd.DataFrame()
            ## get block_start
            if block_start == 'auto':
                block_start_val = self.db_connector.get_max_block(query.table_name)
                print(f'Current max block for {query.key} is {block_start_val}')
            else:
                block_start_val = block_start

            ## run this in a loop until no data is returned 
            while True:
                print(f"...loading raw data for {query.key} with block_start: {block_start_val} and block_end: {block_start_val + query.block_steps}")

                query.update_query_parameters({'block_start': block_start_val, 'block_end': block_start_val + query.block_steps})

                ## trigger query
                payload = json.dumps({"query": query.sql})
                res = api_post_call(self.url, payload=payload, header=self.headers)
                task_id = res['data']['task_id']

                df = pd.DataFrame(res['data']['result'])
                dfMain = pd.concat([dfMain,df])        
                if df.shape[0] > 400:
                    print(f"... started task {task_id} for query. Loaded {dfMain.shape[0]} rows.")    
                else:
                    print(f"DONE loading raw data for {query.key}")
                    break

                ## loop through pages and append query results
                while 'next_page' in res['data']:
                    time.sleep(2)
                    next_page = res['data']['next_page']
                    payload = json.dumps({"task_id": task_id, "page": next_page})
                    res = api_post_call(self.url, payload=payload, header=self.headers)
                    df = pd.DataFrame(res['data']['result'])
                    dfMain = pd.concat([dfMain,df])
                    print(f"... looping through result set for task {task_id}. On page {next_page}. Loaded {dfMain.shape[0]} rows.")

                    if dfMain.shape[0] > 30000:
                        self.upload_data(dfMain, query)
                        block_start_val = dfMain.block_number.max()
                        dfMain = pd.DataFrame()

                print(f"... finished loading task {task_id} for query. Loaded {dfMain.shape[0]} rows.")

                if dfMain.shape[0] > 0:
                    self.upload_data(dfMain, query)
                    ## set new block_start
                    block_start_val = dfMain.block_number.max()
                    dfMain = pd.DataFrame()

    def upload_data(self, df, query):
        ## change columns block_number to int
            df['block_number'] = df['block_number'].astype(int)

            file_name = f"{query.table_name}_{df.block_number.min()}-{df.block_number.max()}"

            dataframe_to_s3(f'{query.s3_folder}/{file_name}', df)

            ## some df preps
            if query.key == 'arbitrum_tx':
                df = df[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'tx_fee', 'status', 'eth_value', 'gas_limit', 'gas_used', 'gas_price_bid', 'gas_price_paid']]
                # replace '0x' in columns ['to_address', 'tx_hash', 'from_address'] in df with '\x'
                df['to_address'] = df['to_address'].str.replace('0x', '\\x', regex=False)
                df['tx_hash'] = df['tx_hash'].str.replace('0x', '\\x', regex=False)
                df['from_address'] = df['from_address'].str.replace('0x', '\\x', regex=False)
            elif query.key == 'optimism_tx':
                df = df[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'tx_fee', 'status', 'eth_value', 'gas_limit', 'gas_price', 'gas_used']]
                # replace '0x' in columns ['to_address', 'tx_hash', 'from_address'] in df with '\x'
                df['to_address'] = df['to_address'].str.replace('0x', '\\x', regex=False)
                df['tx_hash'] = df['tx_hash'].str.replace('0x', '\\x', regex=False)
                df['from_address'] = df['from_address'].str.replace('0x', '\\x', regex=False)
            elif query.key == 'ethereum_tx':
                raise NotImplementedError(f"Query {query.key} not implemented yet")
            else:
                raise NotImplementedError(f"Query {query.key} not implemented yet")

            ## upsert data to db
            df.set_index('tx_hash', inplace=True)
            self.db_connector.upsert_table(query.table_name, df)
            print(f"...upserted {df.shape[0]} rows to {query.table_name} table")
