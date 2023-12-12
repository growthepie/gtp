from src.adapters.abstract_adapters import AbstractAdapterRaw
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from src.adapters.adapter_utils import *

class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc = adapter_params['rpc']
        self.chain = adapter_params['chain']
        self.url = adapter_params['node_url']
        self.table_name = f'{self.chain}_tx'   
 
        # Initialize Web3 connection
        self.w3 = connect_to_node(self.url)
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
                
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.threads = load_params['threads']
        self.run(self.block_start, self.batch_size, self.threads)
        print(f"FINISHED loading raw tx data for {self.chain}.")
        
    def set_rpc_url(self, new_url:str):
        self.node_url = new_url
        
    def run(self, block_start, batch_size, threads):
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")

        if not check_s3_connection(self.s3_connection):
            raise ConnectionError("S3 is not connected.")
        else:
            print("Successfully connected to S3.")

        if not self.w3 or not self.w3.is_connected():
            raise ConnectionError("Not connected to a node.")
        else:
            print("Successfully connected to the node.")

        latest_block = get_latest_block(self.w3)
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")

        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        print(f"Running with start block {block_start} and latest block {latest_block}")

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            
            for current_start in range(block_start, latest_block + 1, batch_size):
                current_end = current_start + batch_size - 1
                if current_end > latest_block:
                    current_end = latest_block

                futures.append(executor.submit(fetch_and_process_range, current_start, current_end, self.chain, self.w3, self.table_name, self.s3_connection, self.bucket_name, self.db_connector))

            # Wait for all threads to complete and handle any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread raised an exception: {e}")
                    traceback.print_exc()