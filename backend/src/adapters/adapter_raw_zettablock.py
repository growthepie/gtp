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
    def extract_raw(self, load_params:dict, if_exists = 'update'):
        ## Set variables
        self.keys = load_params['keys']
        self.block_start = load_params['block_start']
        self.block_end = load_params.get('block_end', None)
        self.step_overwrite = load_params.get('step_overwrite', None)

        self.queries_to_load = [x for x in zettablock_raws if x.key in self.keys]

        ## Trigger queries
        df = self.trigger_check_extract_queries(self.queries_to_load, self.block_start, if_exists, self.block_end, self.step_overwrite)
        return df

    ## ----------------- Helper functions --------------------

    ## identify current max block in ZettaBlock database
    def get_last_block(self, key):        
        query = [x for x in zettablock_raws if x.key == key][0]
        max_block_run_id = self.client.trigger_query(query.max_block_query_id)
        print(f'...finding latest block in ZettaBlock for {query.key} with query_run_id: {max_block_run_id}')
        time.sleep(3)
        self.wait_till_query_done(max_block_run_id)
        block_end = int(self.client.get_query_results(max_block_run_id, single_value=True))
        print(f'Current max block for {key} in ZettaBlock database is {block_end}')
        return block_end

    def trigger_check_extract_queries(self, queries_to_load, block_start, if_exists, block_end = None, step_overwrite = None):
        for query in queries_to_load:            
            dfMain = pd.DataFrame()
            ## get block_start
            if block_start == 'auto':
                block_start_val = self.db_connector.get_max_block(query.table_name)
                print(f'Current max block for {query.key} in our database is {block_start_val}')
            else:
                block_start_val = block_start

            if block_end is None:
                block_end = self.get_last_block(query.key)


            ## run this in a loop until we reach max_block
            while True:
                print(f"...start loading raw data for {query.key} with block_start: {block_start_val}")

                ## trigger query
                if step_overwrite is None or (block_end - block_start_val) > query.steps:
                    payload = {"paramsStr": "{\"params\":[{\"name\":\"block_start\",\"value\":\"" + str(block_start_val) + "\"},{\"name\":\"block_end\",\"value\":\"" + str(block_start_val + query.steps) + "\"}]}"}
                else:
                    payload = {"paramsStr": "{\"params\":[{\"name\":\"block_start\",\"value\":\"" + str(block_start_val) + "\"},{\"name\":\"block_end\",\"value\":\"" + str(block_end+1) + "\"}]}"}
                #print(payload)        
                
                query.last_run_id = self.client.trigger_query(query.query_id, payload)
                query.last_execution_loaded = False        
                print(f'... triggerd query_id: {query.query_id} with query_run_id: {query.last_run_id}. With block_start: {block_start_val}')        
                time.sleep(1)

                ## wait till query done                  
                self.wait_till_query_done(query.last_run_id)

                ## get query results
                df = self.client.get_query_results(query.last_run_id)

                ## check if data is returned
                if df.shape[0] == 0:
                    if step_overwrite is not None:
                        print(f'...no data returned for {query.key} with block_start: {block_start_val} and block_end: {block_end + 1}. ZettaBlock doesnt have data?')
                        break
                    else:
                        print(f'...no data returned for {query.key} with block_start: {block_start_val}. Add {query.steps} to block_start and try again')
                        block_start_val += query.steps
                elif int(df.block_number.max()) >= block_end:
                    print(f'reached the end with start: {block_start_val} and end: {df.block_number.max()}')
                    dfMain = pd.concat([dfMain, df])
                    break
                elif int(df.block_number.max()) == block_start_val:
                    print(f'...loaded {df.shape[0]} rows for {query.key}. No new data though, will add {query.steps} to block_start and try again')
                    dfMain = pd.concat([dfMain, df])
                    block_start_val += query.steps
                else:
                    print(f'...loaded {df.shape[0]} rows for {query.key}')
                    dfMain = pd.concat([dfMain, df])
                    block_start_val = dfMain.block_number.max()

                if dfMain.shape[0] > 50000:
                    print(f'...loaded more than 50k rows for {query.key}, trigger upload')
                    self.upload(dfMain, query, if_exists)
                    dfMain = pd.DataFrame()
            
            ## upload remaining data
            if dfMain.shape[0] > 0:
                self.upload(dfMain, query, if_exists)

            print(f'DONE loading raw data for {query.key}')    
            
    
    # check response until success or failed is returned
    def wait_till_query_done(self, queryrun_id):
        while True:   
            res = self.client.check_query_execution(queryrun_id)
            if res == True:
                return True
            time.sleep(2)

    def upload(self, df, query, if_exists):
        ## drop duplicates based on tx_hash
        df.drop_duplicates(subset=['hash'], inplace=True)

        df.value = df.value.astype('string')
        file_name = f"{query.table_name}_{df.block_number.min()}-{df.block_number.max()}_zettablock"

        ## upload to s3
        dataframe_to_s3(f'{query.s3_folder}/{file_name}', df)

        ## prep data for upsert
        if query.key == 'polygon_zkevm_tx':
            df = self.prepare_dataframe_polygon_zk(df)
        elif query.key == 'zksync_era_tx':            
            df = self.prepare_dataframe_zksync_era(df)
        else:
            print(f'key {query.key} not found')
            raise ValueError        
       

        ## upsert data to db
        df.set_index('tx_hash', inplace=True)
        self.db_connector.upsert_table(query.table_name, df, if_exists)
        print(f"...upserted {df.shape[0]} rows to {query.table_name} table")

    def prepare_dataframe_polygon_zk(self, df):
        # Columns to be used from the dataframe
        cols = ['block_number', 'block_time', 'hash', 'from_address', 'to_address', 'status', 'value', 'gas_limit', 'gas_used', 'gas_price', 'type', 'receipt_contract_address']

        # Filter the dataframe to only include the above columns plus 'input' for calculations
        df = df.loc[:, cols + ['input']]

        # Rename columns
        df.rename(columns={'hash': "tx_hash", "block_time":"block_timestamp"}, inplace=True, errors="ignore")

        # Add tx_fee column
        df['tx_fee'] = df['gas_used'] * df['gas_price']  / 1e18

        # gas_price column: convert to eth
        df['gas_price'] = df['gas_price'].astype(float) / 1e18

        # Add native_transfer column True when (input = 0x OR input is empty) and value > 0 then true else false
        df['value'] = df['value'].astype(float) / 1e18
        
        # Add empty_input column True when input is empty or 0x then true else false
        df['empty_input'] = df['input'].apply(lambda x: True if (x == '0x' or x == '') else False)

        # status column: 1 if status is success, 0 if failed else -1
        df['status'] = df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

        # Convert block_time to datetime
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])

        # Handle bytea data type
        for col in ['tx_hash', 'to_address', 'from_address', 'receipt_contract_address']:
            if col in df.columns:
                if df[col].dtype == 'string':
                    df[col] = df[col].str.replace('0x', '\\x', regex=False)
                elif df[col].dtype == 'object':
                    ## change type to str
                    df[col] = df[col].astype('string')
                    df[col] = df[col].str.replace('0x', '\\x', regex=False)
                else:
                    print(f'column {col} is not of type string, but {df[col].dtype}')
            else:
                print(f"Column {col} not found in dataframe.")
        
        # Drop the 'input' column
        df = df.drop(columns=['input'])

        return df
    

    def prepare_dataframe_zksync_era(self, df):
        # Columns to be used from the dataframe
        cols = ['block_number', 'block_time', 'hash', 'from_address', 'to_address', 'status', 'value', 'gas_limit', 'gas_used', 'gas_price', 'type', 'receipt_contract_address']

        # Filter the dataframe to only include the above columns plus 'input' for calculations
        df = df.loc[:, cols + ['input']]

        # Rename columns
        df.rename(columns={'hash': "tx_hash", "block_time":"block_timestamp"}, inplace=True, errors="ignore")

        # Add tx_fee column
        df['tx_fee'] = df['gas_used'] * df['gas_price']  / 1e18

        # gas_price column: convert to eth
        df['gas_price'] = df['gas_price'].astype(float) / 1e18

        # value column divide by 1e18 to convert to eth
        df['value'] = df['value'].astype(float) / 1e18

        # Add empty_input column True when input is empty or 0x then true else false
        df['empty_input'] = df['input'].apply(lambda x: True if (x == '0x' or x == '') else False)

        # status column: 1 if status is success, 0 if failed else -1
        df['status'] = df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

        # Convert block_time to datetime
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])

        # Handle bytea data type
        for col in ['tx_hash', 'to_address', 'from_address', 'receipt_contract_address']:
            if col in df.columns:
                if df[col].dtype == 'string':
                    df[col] = df[col].str.replace('0x', '\\x', regex=False)
                elif df[col].dtype == 'object':
                    ## change type to str
                    df[col] = df[col].astype('string')
                    df[col] = df[col].str.replace('0x', '\\x', regex=False)
                else:
                    print(f'column {col} is not of type string, but {df[col].dtype}')
            else:
                print(f"Column {col} not found in dataframe.")
        
        # Drop the 'input' column
        df = df.drop(columns=['input'])

        return df