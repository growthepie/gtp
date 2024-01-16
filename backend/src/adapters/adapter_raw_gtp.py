from src.adapters.abstract_adapters import AbstractAdapterRaw
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from src.adapters.adapter_utils import *

class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc_urls = adapter_params['rpc_urls']
        
        # Set max_calls_per_rpc only if it's part of adapter_params
        if 'max_calls_per_rpc' in adapter_params:
            self.max_calls_per_rpc = adapter_params['max_calls_per_rpc']
        else:
            self.max_calls_per_rpc = None

        self.current_rpc_index = 0
        self.current_call_count = 0
        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
 
        # Initialize Web3 connection with the first RPC URL
        self.w3 = connect_to_node(self.rpc_urls[self.current_rpc_index])
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
        
    def rotate_rpc_url(self):
        # If only one RPC URL is provided, do not rotate
        if len(self.rpc_urls) == 1:
            return
        self.current_rpc_index = (self.current_rpc_index + 1) % len(self.rpc_urls)
        self.current_call_count = 0  # Reset call count for new RPC
        self.w3 = connect_to_node(self.rpc_urls[self.current_rpc_index])

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
                # Only check and rotate if multiple RPC URLs are provided and max_calls_per_rpc is set
                if len(self.rpc_urls) > 1 and self.max_calls_per_rpc:
                    max_calls_for_current_rpc = self.max_calls_per_rpc.get(self.rpc_urls[self.current_rpc_index], float('inf'))
                    if self.current_call_count >= max_calls_for_current_rpc:
                        self.rotate_rpc_url()

                current_end = current_start + batch_size - 1
                if current_end > latest_block:
                    current_end = latest_block

                futures.append(executor.submit(fetch_and_process_range, current_start, current_end, self.chain, self.w3, self.table_name, self.s3_connection, self.bucket_name, self.db_connector))
                self.current_call_count += 1  # Increment call count

            # Wait for all threads to complete and handle any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread raised an exception: {e}")
                    traceback.print_exc()