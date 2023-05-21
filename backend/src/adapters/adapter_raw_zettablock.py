import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.queries.zettablock_queries import zettablock_raws 
from src.adapters.clients.zettablock_api import ZettaBlock_API
from src.misc.helper_functions import print_init, dataframe_to_s3

##ToDos: 
# Add days parameter once functionality is available & then also better logic for days to load

class AdapterZettaBlockRaw(AbstractAdapterRaw):
    """
    adapter_params require the following fields:
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ZettablockRaw", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']
        self.client = ZettaBlock_API(self.api_key)

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

        self.queries_to_load = [x for x in zettablock_raws if x.key in self.keys]

        ## Trigger queries
        df = self.trigger_check_extract_queries(self.queries_to_load, self.block_start)
        return df

    ## ----------------- Helper functions --------------------

    def trigger_check_extract_queries(self, queries_to_load, block_start):
        for query in queries_to_load:            
            dfMain = pd.DataFrame()
            ## get block_start
            if block_start == 'auto':
                block_start_val = self.db_connector.get_max_block(query.table_name)
                print(f'Current max block for {query.key} is {block_start_val}')
            else:
                block_start_val = block_start

            ## run this in a loop until no data is returned
            while True:
                print(f"...start loading raw data for {query.key} with block_start: {block_start_val}")

                ## trigger query
                payload = {"paramsStr": "{\"params\":[{\"name\":\"block_start\",\"value\":\"" + str(block_start_val) + "\"},{\"name\":\"block_end\",\"value\":\"" + str(block_start_val + query.steps) + "\"}]}"}
                query.last_run_id = self.client.trigger_query(query.query_id, payload)
                query.last_execution_loaded = False        
                print(f'... triggerd query_id: {query.query_id} with query_run_id: {query.last_run_id}. With block_start: {block_start_val}')        
                time.sleep(3)

                ## wait till query done                  
                self.wait_till_query_done(query.last_run_id)

                ## get query results
                df = self.client.get_query_results(query.last_run_id)

                ## check if data is returned
                if df.shape[0] < 1:
                    print(f'no data with block_start: {block_start_val}')
                    break
                elif df.block_number.max() == block_start_val:
                    print(f'no new data with block_start: {block_start_val}')
                    break
                else:
                    print(f'...loaded {df.shape[0]} rows for {query.key}')
                    dfMain = pd.concat([dfMain, df])
                    block_start_val = dfMain.block_number.max()

                if dfMain.shape[0] > 50000:
                    print(f'...loaded more than 50k rows for {query.key}, trigger upload')
                    self.upload(dfMain, query)
                    dfMain = pd.DataFrame()
            
            ## upload remaining data
            if dfMain.shape[0] > 0:
                self.upload(dfMain, query)

            print(f'DONE loading raw data for {query.key}')    
            
    
    # check response until success or failed is returned
    def wait_till_query_done(self, queryrun_id):
        while True:   
            res = self.client.check_query_execution(queryrun_id)
            if res == True:
                return True
            time.sleep(2)

    def upload(self, df, query):
        ## drop duplicates based on tx_hash
        df.drop_duplicates(subset=['hash'], inplace=True)

        df.value = df.value.astype('string')
        file_name = f"{query.table_name}_{df.block_number.min()}-{df.block_number.max()}"

        ## upload to s3
        dataframe_to_s3(f'{query.s3_folder}/{file_name}', df)

        ## prep data for upsert
        df.rename(columns={'hash': "tx_hash", "from": "from_address", "to": "to_address", "value": "eth_value", "block_time":"block_timestamp"}, inplace=True, errors="ignore")
        df = df[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'receipt_contract_address', 'status', 'eth_value', 'gas_limit', 'gas_used', 'gas_price', 'type']]
        
        df['tx_hash'] = df['tx_hash'].str.replace('"', '', regex=False)
        df['tx_hash'] = df['tx_hash'].str.replace('[', '', regex=False) 

        ## prepare hex values for upsert
        for col in ['tx_hash', 'to_address', 'from_address', 'receipt_contract_address']:
            ## check if column is type string
            if df[col].dtype == 'string':
                df[col] = df[col].str.replace('0x', '\\x', regex=False)
            else:
                print(f'column {col} is not of type string, but {df[col].dtype}')

        ## upsert data to db
        df.set_index('tx_hash', inplace=True)
        self.db_connector.upsert_table(query.table_name, df)
        print(f"...upserted {df.shape[0]} rows to {query.table_name} table")
