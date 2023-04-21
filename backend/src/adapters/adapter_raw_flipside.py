import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.queries.flipside_queries import flipside_raws
from src.adapters.clients.flipside_api import FlipsideAPI
from src.misc.helper_functions import print_init, dataframe_to_s3

##ToDos: 
# Add days parameter once functionality is available & then also better logic for days to load

class AdapterFlipsideRaw(AbstractAdapterRaw):
    """
    adapter_params require the following fields:
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("FlipsideRaw", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']

        self.client = FlipsideAPI(self.api_key)
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

        self.queries_to_load = [x for x in flipside_raws if x.key in self.keys]

        ## Trigger queries
        self.trigger_check_extract_queries(self.queries_to_load, self.block_start)
    

    ## ----------------- Helper functions --------------------

    def trigger_check_extract_queries(self, queries_to_load, block_start):
        sleeper = 5
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
                query.update_query_parameters({'block_start': block_start_val, 'block_end': block_start_val + query.block_steps})
                response_json = self.client.create_query(query.sql)
                query.last_token = response_json.get('token')
                query.last_execution_loaded = False                
                time.sleep(sleeper)

                ## checke query status
                wait_time = 0
                while True:                    
                    resp = self.client.check_query_execution(query.last_token)
                    if resp == False:
                        print(f"...wait for {query.key}.")
                        wait_time += sleeper
                        if wait_time > 180: ##if wait longer than 3 minutes, retrigger query
                            print(f"waited too long for {query.key}. retriggering")
                            response_json = self.client.create_query(query.sql)
                            query.last_token = response_json.get('token')
                            query.last_execution_loaded = False  
                            wait_time = 0      
                        time.sleep(sleeper)                   
                    elif resp == True:
                        query.last_execution_loaded = True
                        print(f'...load done for {query.key} with block_start: {block_start_val} and block_end: {block_start_val + query.block_steps}')
                        break
                    else:
                        print(f"issue with {query.key}")
                        query.last_execution_loaded = True
                        query.execution_error = True
                        print(resp)
                        raise Exception("Error in query execution")
                
                ## Loop over query results
                for i in range(1, 10):                    
                    response_json = self.client.get_query_results(query.last_token, page_number=i)
                    df = pd.DataFrame(response_json['results'], columns=response_json['columnLabels'])
                    df_length = df.shape[0]
                    if i == 1 and df_length == 0:
                        print(f"ALL loaded for {query.key}")
                        all_done = True
                        break
                    else:
                        if df_length == 0:
                            break
                        else:
                            print(f'...loaded {df_length} rows for {query.key}')
                            dfMain = pd.concat([dfMain, df])

                if all_done == True:
                    break
                else:
                    file_name = f"{query.table_name}_{dfMain.BLOCK_NUMBER.min()}-{dfMain.BLOCK_NUMBER.max()}"

                    ## store locally
                    # dfMain.to_parquet(f"output/raw_data/flipside/{query.table_name}_{dfMain.BLOCK_NUMBER.min()}-{dfMain.BLOCK_NUMBER.max()}.parquet", index=False)
                    # print(f"...stored file locally: {query.table_name}_{dfMain.BLOCK_NUMBER.min()}-{dfMain.BLOCK_NUMBER.max()}.parquet")

                    dataframe_to_s3(f'{query.s3_folder}/{file_name}', dfMain)

                    ## some df preps
                    dfMain.columns = [x.lower() for x in dfMain.columns]

                    if query.key == 'arbitrum_tx':
                        dfMain = dfMain[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'tx_fee', 'status', 'eth_value', 'gas_limit', 'gas_used', 'gas_price_bid', 'gas_price_paid']]
                        # replace '0x' in columns ['to_address', 'tx_hash', 'from_address'] in df with '\x'
                        dfMain['to_address'] = dfMain['to_address'].str.replace('0x', '\\x', regex=False)
                        dfMain['tx_hash'] = dfMain['tx_hash'].str.replace('0x', '\\x', regex=False)
                        dfMain['from_address'] = dfMain['from_address'].str.replace('0x', '\\x', regex=False)
                    elif query.key == 'optimism_tx':
                        dfMain = dfMain[['block_number', 'block_timestamp', 'tx_hash', 'from_address', 'to_address', 'tx_fee', 'status', 'eth_value', 'gas_limit', 'gas_price', 'gas_used']]
                        # replace '0x' in columns ['to_address', 'tx_hash', 'from_address'] in df with '\x'
                        dfMain['to_address'] = dfMain['to_address'].str.replace('0x', '\\x', regex=False)
                        dfMain['tx_hash'] = dfMain['tx_hash'].str.replace('0x', '\\x', regex=False)
                        dfMain['from_address'] = dfMain['from_address'].str.replace('0x', '\\x', regex=False)
                    elif query.key == 'ethereum_tx':
                        raise NotImplementedError(f"Query {query.key} not implemented yet")
                    else:
                        raise NotImplementedError(f"Query {query.key} not implemented yet")

                    ## upsert data to db
                    dfMain.set_index('tx_hash', inplace=True)
                    self.db_connector.upsert_table(query.table_name, dfMain)
                    print(f"...upserted {dfMain.shape[0]} rows to {query.table_name} table")

                    ## set new block_start
                    block_start_val += query.block_steps
                    dfMain = pd.DataFrame()