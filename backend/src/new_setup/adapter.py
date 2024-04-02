from src.adapters.abstract_adapters import AbstractAdapterRaw
import traceback
from queue import Queue
from threading import Thread
from src.new_setup.utils import *

class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc_configs = adapter_params.get('rpc_configs', [])

        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        
        # Initialize Web3 connection
        self.w3 = Web3CC(self.rpc_configs[0])
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
            
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.run(self.block_start, self.batch_size)
        print(f"FINISHED loading raw tx data for {self.chain}.")
        
    def run(self, block_start, batch_size):
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")

        if not check_s3_connection(self.s3_connection):
            raise ConnectionError("S3 is not connected.")
        else:
            print("Successfully connected to S3.")

        latest_block = get_latest_block(self.w3)
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")

        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        print(f"Running with start block {block_start} and latest block {latest_block}")
        # Initialize the block range queue
        block_range_queue = Queue()
        
        # Calculate and enqueue all block ranges
        for current_start in range(block_start, latest_block + 1, batch_size):
            current_end = min(current_start + batch_size - 1, latest_block)
            block_range_queue.put((current_start, current_end))
            
        # Create and start a thread for each RPC URL in the rpc_configs
        threads = []
        for rpc_config in self.rpc_configs:
            thread = Thread(target=self.process_rpc_config, args=(rpc_config, block_range_queue,))
            threads.append(thread)
            thread.start()
            print(f"Started thread for {rpc_config['url']}")

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
    def process_rpc_config(self, rpc_config, block_range_queue):
        max_retries = 5
        retry_delay = 10
        attempt = 0

        # Try to connect to the node, with retries
        while attempt < max_retries:
            try:
                print(f"Attempting to connect to {rpc_config['url']}, attempt {attempt + 1}")
                node_connection = connect_to_node(rpc_config)
                print(f"Successfully connected to {rpc_config['url']}")
                break
            except Exception as e:
                print(f"Failed to connect to {rpc_config['url']}: {e}")
                attempt += 1
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Failed to connect to {rpc_config['url']} after {max_retries} attempts. Skipping this RPC.")
                    return

        workers = [Thread(target=self.worker_task, args=(rpc_config, node_connection, block_range_queue,))
                   for _ in range(rpc_config['workers'])]

        for worker in workers:
            worker.start()
            print(f"Started worker {workers.index(worker)+1} for {rpc_config['url']}")

        for worker in workers:
            worker.join()
            
    def worker_task(self, rpc_config, node_connection, block_range_queue):
        while not block_range_queue.empty():
            block_range = None
            try:
                block_range = block_range_queue.get_nowait()
                print(f"Processing block range {block_range[0]}-{block_range[1]} from {rpc_config['url']}")
                fetch_and_process_range(block_range[0], block_range[1], self.chain, node_connection, self.table_name, self.s3_connection, self.bucket_name, self.db_connector)
            except Exception as e:
                print(f"Worker raised an exception: {e}")
                traceback.print_exc()
            finally:
                if block_range:
                    block_range_queue.task_done()