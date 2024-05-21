from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.adapters.funcs_backfill import check_and_record_missing_block_ranges
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

        if block_start > latest_block:
            raise ValueError("The start block cannot be higher than the latest block.")

        print(f"Running with start block {block_start} and latest block {latest_block}")

        # Initialize the block range queue
        block_range_queue = Queue()
        self.enqueue_block_ranges(block_start, latest_block, batch_size, block_range_queue)
        print(f"Enqueued {block_range_queue.qsize()} block ranges.")
        self.manage_threads(block_range_queue)     

    def enqueue_block_ranges(self, block_start, block_end, batch_size, queue):
        for start in range(block_start, block_end + 1, batch_size):
            end = min(start + batch_size - 1, block_end)
            queue.put((start, end))

    def manage_threads(self, block_range_queue):
        threads = []
        rpc_errors = {}  # Track errors for each RPC configuration
        error_lock = Lock()
        
        for rpc_config in self.rpc_configs:
            rpc_errors[rpc_config['url']] = 0
            self.active_rpcs.add(rpc_config['url'])  # Mark as active
            thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                rpc, block_range_queue, rpc_errors, error_lock))
            threads.append((rpc_config['url'], thread))
            thread.start()
            print(f"Started thread for {rpc_config['url']}")

        # Start the monitoring thread with access to RPC configurations
        monitor_thread = Thread(target=self.monitor_workers, args=(
            threads, block_range_queue, self.rpc_configs, rpc_errors, error_lock))
        monitor_thread.start()
        print("Started monitoring thread.")
        
        monitor_thread.join()
        
        print("All worker and monitoring threads have completed.")

    def monitor_workers(self, threads, block_range_queue, rpc_configs, rpc_errors, error_lock):
        additional_threads = []
        while True:
            active_threads = [thread for _, thread in threads if thread.is_alive()]
            active = bool(active_threads)

            # Check if the block range queue is empty and no threads are active
            if block_range_queue.empty() and not active:
                print("DONE: All workers have stopped and the queue is empty.")
                break
            
            # Check if there are no more block ranges to process, but threads are still active
            if block_range_queue.qsize() == 0 and active:
                print("...no more block ranges to process. Waiting for workers to finish.")
            else:
                print(f"====> Block range queue size: {block_range_queue.qsize()}. Active threads: {len(active_threads)}")
                
            if not block_range_queue.empty() and not active:
                print("Detected unfinished tasks with no active workers. Restarting worker.")
                # Filter and restart workers for specific RPC configs
                for rpc_config in rpc_configs:
                    # if rpc_config['workers'] > 5:
                    print(f"Restarting workers for RPC URL: {rpc_config['url']}")
                    new_thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                        rpc, block_range_queue, rpc_errors, error_lock))
                    threads.append((rpc_config['url'], new_thread))
                    additional_threads.append(new_thread)
                    new_thread.start()
                    break

            time.sleep(5)  # Sleep to avoid high CPU usage

        # Join all initial threads
        for _, thread in threads:
            thread.join()
            print(f"Thread for RPC URL has completed.")

        # Join all additional threads if any were started
        for thread in additional_threads:
            thread.join()
            print("Additional worker thread has completed.")

        print("All worker and monitoring threads have completed.")
            
    def process_rpc_config(self, rpc_config, block_range_queue, rpc_errors, error_lock):
        max_retries = 3
        retry_delay = 10
        attempt = 0

        # Try to connect to the node, with retries
        node_connection = None
        while attempt < max_retries:
            try:
                #print(f"Attempting to connect to {rpc_config['url']}, attempt {attempt + 1}")
                node_connection = connect_to_node(rpc_config)
                print(f"Successfully connected to {rpc_config['url']}")
                break
            except Exception as e:
                #print(f"Failed to connect to {rpc_config['url']}: {e}")
                attempt += 1
                if attempt >= max_retries:
                    #print(f"Failed to connect to {rpc_config['url']} after {max_retries} attempts. Skipping this RPC.")
                    return
                #print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        if not node_connection:
            return  # If the connection failed after retries, exit the function.

        workers = []
        for _ in range(rpc_config['workers']):
            worker = Thread(target=self.worker_task, args=(rpc_config, node_connection, block_range_queue, rpc_errors, error_lock))
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
                block_range = block_range_queue.get(timeout=5)
                print(f"...processing block range {block_range[0]}-{block_range[1]} from {rpc_config['url']}")
                fetch_and_process_range(block_range[0], block_range[1], self.chain, node_connection, self.table_name, self.s3_connection, self.bucket_name, self.db_connector, rpc_config['url'])
            except Empty:
                print("DONE: no more blocks to process. Worker is shutting down.")
                return
            except Exception as e:
                with error_lock:
                    rpc_errors[rpc_config['url']] += 1  # Increment error count
                    # Check immediately if the RPC should be removed
                    if rpc_errors[rpc_config['url']] >= len([rpc['workers'] for rpc in self.rpc_configs if rpc['url'] == rpc_config['url']]):
                        print(f"All workers for {rpc_config['url']} failed. Removing this RPC from rotation.")
                        self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_config['url']]
                        self.active_rpcs.remove(rpc_config['url'])
                print(f"ERROR: for {rpc_config['url']} on block range {block_range[0]}-{block_range[1]}: {e}")
                block_range_queue.put(block_range)  # Re-queue the failed block range
                print(f"RE-QUEUED: block range {block_range[0]}-{block_range[1]}")
                break
            finally:
                if block_range:
                    block_range_queue.task_done()

    def process_missing_blocks(self, missing_block_ranges, batch_size):
        block_range_queue = Queue()
        for start, end in missing_block_ranges:
            self.enqueue_block_ranges(start, end, batch_size, block_range_queue)
        self.manage_threads(block_range_queue)
    
    def fetch_block_transaction_count(self, w3, block_num):
        block = w3.eth.get_block(block_num, full_transactions=False)
        return len(block['transactions'])

    def backfill_missing_blocks(self, start_block, end_block, batch_size):
        missing_block_ranges = check_and_record_missing_block_ranges(self.db_connector, self.table_name, start_block, end_block)
        if not missing_block_ranges:
            print("No missing block ranges found.")
            return
        print(f"Found {len(missing_block_ranges)} missing block ranges.")
        print("Filtering out ranges with 0 transactions...")
        filtered_ranges = []
        for start, end in missing_block_ranges:
            if start == end:
                transaction_count = self.fetch_block_transaction_count(self.w3, start)
                if transaction_count > 0:
                    filtered_ranges.append((start, end))
            else:
                filtered_ranges.append((start, end))

        if len(filtered_ranges) == 0:
            print("No missing block ranges found.")
            return
        
        else:
            print("Backfilling missing blocks")
            print("Missing block ranges:")
            for start, end in filtered_ranges:
                print(f"{start}-{end}")
            self.process_missing_blocks(filtered_ranges, batch_size)