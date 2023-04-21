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
            all_done = False
            dfMain = pd.DataFrame()
            ## get block_start
            if block_start == 'auto':
                block_start_val = self.db_connector.get_max_block(query.table_name)
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
                else:
                    print(f'...loaded {df.shape[0]} rows for {query.key}')
                    dfMain = pd.concat([dfMain, df])
                    block_start_val += query.steps
            

            dfMain.value = dfMain.value.astype('string')
            file_name = f"{query.table_name}_{dfMain.block_number.min()}-{dfMain.block_number.max()}"

            ## store locally
            # dfMain.to_parquet(f"output/raw_data/polygon_zkevm/{file_name}.parquet", index=False)
            # print(f"...stored file locally: {file_name}.parquet")

            ## upload to s3
            dataframe_to_s3(f'{query.s3_folder}/{file_name}', dfMain)

            ## prep data for upsert
            dfMain.rename(columns={'hash': "tx_hash", "from": "from_address", "to": "to_address", "value": "eth_value", "block_time":"block_timestamp"}, inplace=True)
            dfMain = dfMain[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'receipt_contract_address', 'status', 'eth_value', 'gas_limit', 'gas_used', 'gas_price', 'type']]
            dfMain['tx_hash'] = dfMain['tx_hash'].str.replace('"', '', regex=False)
            dfMain['tx_hash'] = dfMain['tx_hash'].str.replace('[', '', regex=False) 

            ## upsert data to db
            dfMain.set_index('tx_hash', inplace=True)
            self.db_connector.upsert_table(query.table_name, dfMain)
            print(f"...upserted {dfMain.shape[0]} rows to {query.table_name} table")
    
    # check response until success or failed is returned
    def wait_till_query_done(self, queryrun_id):
        while True:   
            res = self.client.check_query_execution(queryrun_id)
            if res == True:
                return True
            time.sleep(2)
