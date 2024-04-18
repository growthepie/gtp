from src.adapters.abstract_adapters import AbstractAdapterRaw
from queue import Queue, Empty
from threading import Thread, Lock
from src.new_setup.utils import *

class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc_configs = adapter_params.get('rpc_configs', [])
        if not self.rpc_configs:
            raise ValueError("No RPC configurations provided.")
        
        self.active_rpcs = set()  # Keep track of active RPC configurations

        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        
        # Try to initialize Web3 connection with the provided RPC configs
        self.w3 = None
        for rpc_config in self.rpc_configs:
            try:
                self.w3 = Web3CC(rpc_config)
                print(f"Connected to RPC URL: {rpc_config['url']}")
                break
            except Exception as e:
                print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")
        
        if self.w3 is None:
            raise ConnectionError("Failed to connect to any provided RPC node.")
        
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
        rpc_errors = {}  # Track errors for each RPC configuration
        error_lock = Lock()
        
        for rpc_config in self.rpc_configs:
            rpc_errors[rpc_config['url']] = 0
            self.active_rpcs.add(rpc_config['url'])  # Mark as active
            thread = Thread(target=lambda: self.process_rpc_config(rpc_config, block_range_queue, rpc_errors, error_lock))
            threads.append((rpc_config['url'], thread))
            thread.start()
            print(f"Started thread for {rpc_config['url']}")

        # Wait for all threads to complete
        for _, thread in threads:
            thread.join()
        
        # Check and remove failed RPC configurations
        failed_rpc_urls = [rpc_url for rpc_url, errors in rpc_errors.items() if errors >= len(self.rpc_configs[rpc_url]['workers'])]
        for rpc_url in failed_rpc_urls:
            print(f"All workers for {rpc_url} failed. Removing this RPC from rotation.")
            self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_url]
            self.active_rpcs.remove(rpc_url)  # Mark as inactive
            
    def process_rpc_config(self, rpc_config, block_range_queue, rpc_errors, error_lock):
        max_retries = 5
        retry_delay = 10
        attempt = 0

        # Try to connect to the node, with retries
        node_connection = None
        while attempt < max_retries:
            try:
                print(f"Attempting to connect to {rpc_config['url']}, attempt {attempt + 1}")
                node_connection = connect_to_node(rpc_config)
                print(f"Successfully connected to {rpc_config['url']}")
                break
            except Exception as e:
                print(f"Failed to connect to {rpc_config['url']}: {e}")
                attempt += 1
                if attempt >= max_retries:
                    print(f"Failed to connect to {rpc_config['url']} after {max_retries} attempts. Skipping this RPC.")
                    return
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        if not node_connection:
            return  # If the connection failed after retries, exit the function.

        workers = []
        for _ in range(rpc_config['workers']):
            worker = Thread(target=self.worker_task, args=(rpc_config, node_connection, block_range_queue, rpc_errors, error_lock,))
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()
            if not worker.is_alive():
                print(f"Worker for {rpc_config['url']} has stopped.")
            
    def worker_task(self, rpc_config, node_connection, block_range_queue, rpc_errors, error_lock):
        while rpc_config['url'] in self.active_rpcs and not block_range_queue.empty():
            block_range = None
            try:
                block_range = block_range_queue.get(timeout=10)
                print(f"Processing block range {block_range[0]}-{block_range[1]} from {rpc_config['url']}")
                fetch_and_process_range(block_range[0], block_range[1], self.chain, node_connection, self.table_name, self.s3_connection, self.bucket_name, self.db_connector, rpc_config['url'])
            except Empty:
                print("No more blocks to process. Worker is shutting down.")
                return
            except Exception as e:
                with error_lock:
                    rpc_errors[rpc_config['url']] += 1  # Increment error count
                print(f"Error for {rpc_config['url']} on block range {block_range[0]}-{block_range[1]}: {e}")
                block_range_queue.put(block_range)  # Re-queue the failed block range
                print(f"Re-queued block range {block_range[0]}-{block_range[1]}")
                break
            finally:
                if block_range:
                    block_range_queue.task_done()