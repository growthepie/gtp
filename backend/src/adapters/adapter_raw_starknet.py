from src.adapters.rpc_funcs.funcs_backfill import check_and_record_missing_block_ranges
from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.adapters.rpc_funcs.utils import connect_to_s3, check_s3_connection, handle_retry_exception, check_db_connection, save_data_for_range
from queue import Queue, Empty
from threading import Thread, Lock
import requests
import pandas as pd
import json
import time
import random

class AdapterStarknet(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("StarkNet", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        self.db_connector = db_connector

        self.rpc_configs = adapter_params.get('rpc_configs', [])
        if not self.rpc_configs:
            raise ValueError("No RPC configurations provided.")
        self.active_rpcs = set()

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

        latest_block = None
        for rpc_config in self.rpc_configs:
            try:
                latest_block = self.get_latest_block_id(rpc_config['url'])
                print(f"Connected to RPC URL: {rpc_config['url']}")
                break
            except Exception as e:
                print(f"Failed to get latest block from RPC URL: {rpc_config['url']} with error: {e}")

        if latest_block is None:
            raise ConnectionError("Failed to connect to any provided RPC node.")

        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)
        else:
            block_start = int(block_start)

        if block_start > latest_block:
            raise ValueError("The start block cannot be higher than the latest block.")

        print(f"Running with start block {block_start} and latest block {latest_block}")

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
        rpc_errors = {}
        error_lock = Lock()
        
        for rpc_config in self.rpc_configs:
            rpc_errors[rpc_config['url']] = 0
            self.active_rpcs.add(rpc_config['url'])
            thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                rpc, block_range_queue, rpc_errors, error_lock))
            threads.append((rpc_config['url'], thread))
            thread.start()
            print(f"Started thread for {rpc_config['url']}")

        monitor_thread = Thread(target=self.monitor_workers, args=(
            threads, block_range_queue, self.rpc_configs, rpc_errors, error_lock))
        monitor_thread.start()
        print("Started monitoring thread.")
        
        monitor_thread.join()
        
        print("All worker and monitoring threads have completed.")

    def monitor_workers(self, threads, block_range_queue, rpc_configs, rpc_errors, error_lock):
        additional_threads = []
        while True:
            active_threads = [(rpc_url, thread) for rpc_url, thread in threads if thread.is_alive()]
            active_rpc_urls = [rpc_url for rpc_url, _ in active_threads]
            active = bool(active_threads)

            if block_range_queue.empty() and not active:
                print("DONE: All workers have stopped and the queue is empty.")
                break

            if block_range_queue.qsize() == 0 and active:
                combined_rpc_urls = ", ".join(active_rpc_urls)
                print(f"...no more block ranges to process. Waiting for workers to finish. Active RPCs: {combined_rpc_urls}")
            else:
                print(f"====> Block range queue size: {block_range_queue.qsize()}. #Active threads: {len(active_threads)}")

            if not block_range_queue.empty() and not active:
                print("Detected unfinished tasks with no active workers. Restarting worker.")

                active_rpcs = [rpc_config for rpc_config in rpc_configs if rpc_config['url'] in self.active_rpcs]
                if not active_rpcs:
                    raise Exception("No active RPCs available. Stopping the DAG.")

                for rpc_config in active_rpcs:
                    print(f"Restarting workers for RPC URL: {rpc_config['url']}")
                    new_thread = Thread(target=lambda rpc=rpc_config: self.process_rpc_config(
                        rpc, block_range_queue, rpc_errors, error_lock))
                    threads.append((rpc_config['url'], new_thread))
                    additional_threads.append(new_thread)
                    new_thread.start()
                    break

            time.sleep(5)

        for _, thread in threads:
            thread.join()
            print(f"Thread for RPC URL has completed.")

        for thread in additional_threads:
            thread.join()
            print("Additional worker thread has completed.")

        print("All worker and monitoring threads have completed.")

    def process_rpc_config(self, rpc_config, block_range_queue, rpc_errors, error_lock):
        workers = []
        workers_count = rpc_config.get('workers', 1)
        for _ in range(workers_count):
            worker = Thread(target=self.worker_task, args=(rpc_config, block_range_queue, rpc_errors, error_lock))
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()
            if not worker.is_alive():
                print(f"Worker for {rpc_config['url']} has stopped.")

    def worker_task(self, rpc_config, block_range_queue, rpc_errors, error_lock):
        while rpc_config['url'] in self.active_rpcs and not block_range_queue.empty():
            block_range = None
            try:
                block_range = block_range_queue.get(timeout=5)
                print(f"...processing block range {block_range[0]}-{block_range[1]} from {rpc_config['url']}")
                self.fetch_and_process_range_starknet(block_range[0], block_range[1], self.chain, self.bucket_name, self.db_connector, rpc_config['url'])
            except Empty:
                print("DONE: no more blocks to process. Worker is shutting down.")
                return
            except Exception as e:
                with error_lock:
                    rpc_errors[rpc_config['url']] += 1
                    total_workers = sum([rpc.get('workers',1) for rpc in self.rpc_configs if rpc['url'] == rpc_config['url']])
                    if rpc_errors[rpc_config['url']] >= total_workers:
                        print(f"All workers for {rpc_config['url']} failed. Removing this RPC from rotation.")
                        self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_config['url']]
                        self.active_rpcs.remove(rpc_config['url'])
                print(f"ERROR: for {rpc_config['url']} on block range {block_range[0]}-{block_range[1]}: {e}")
                block_range_queue.put(block_range)
                print(f"RE-QUEUED: block range {block_range[0]}-{block_range[1]}")
                break
            finally:
                if block_range:
                    block_range_queue.task_done()

    def get_latest_block_id(self, rpc_url):
        method = "starknet_blockNumber"
        response = self.send_request(method, rpc_url=rpc_url)
        if 'result' in response:
            return response['result']
        else:
            raise Exception("Failed to retrieve the latest block number.")

    def fetch_and_process_range_starknet(self, current_start, current_end, chain, bucket_name, db_connector, rpc_url):
        base_wait_time = 5
        while True:
            try:
                transactions_df, events_df = self.fetch_starknet_data_for_range(current_start, current_end, rpc_url)
                
                # Check if both DataFrames are empty
                if transactions_df.empty and events_df.empty:
                    print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                    return

                # Process and save transactions
                if not transactions_df.empty:
                    save_data_for_range(transactions_df, current_start, current_end, chain, bucket_name)
                    self.insert_data_into_db(transactions_df, db_connector, 'starknet_tx', 'transaction')
                
                # # Process and save events
                # if not events_df.empty:
                #     self.insert_data_into_db(events_df, db_connector, 'starknet_events', 'event')
                
                print(f"Data inserted for blocks {current_start} to {current_end} successfully.")
                break

            except Exception as e:
                print(f"Error processing blocks {current_start} to {current_end}: {e}")
                base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time, rpc_url)

    def fetch_starknet_data_for_range(self, current_start, current_end, rpc_url):
        print(f"Fetching data for blocks {current_start} to {current_end}...")
        all_blocks_dfs = []
        all_events_dfs = []
        strketh_price = self.db_connector.get_last_price_eth('starknet', 'hourly')
        print(f"pulled latest STRK/ETH price: {strketh_price}")

        for block_id in range(current_start, current_end + 1):
            try:
                full_block_data, events_df = self.get_block_data_and_events(block_id, rpc_url)
                if full_block_data is None:
                    continue
                filtered_df = self.prep_starknet_data(full_block_data, strketh_price)

                # Only append if DataFrame is not empty
                if not filtered_df.empty:
                    all_blocks_dfs.append(filtered_df)
                if not events_df.empty:
                    all_events_dfs.append(events_df)
            except Exception as e:
                print(f"Error fetching data for block {block_id}: {str(e)}")

        # Concatenate all blocks DataFrames if the list is not empty
        if all_blocks_dfs:
            all_blocks_df = pd.concat(all_blocks_dfs, ignore_index=True)
        else:
            all_blocks_df = pd.DataFrame()
            print("No block data available for the given range.")
        
        # Concatenate all events DataFrames if the list is not empty
        if all_events_dfs:
            all_events_df = pd.concat(all_events_dfs, ignore_index=True)
        else:
            all_events_df = pd.DataFrame()
            print("No events data available for the given range.")

        return all_blocks_df, all_events_df

    def get_block_data_and_events(self, block_id, rpc_url):
        try:
            # Use starknet_getBlockWithReceipts to get the block data with receipts
            block_data = self.get_block_with_receipts(block_id, rpc_url)
            transactions_with_receipts = block_data.get('transactions', [])
            
            if not transactions_with_receipts:
                print(f"No transactions found for block {block_id}.")
                return None, pd.DataFrame()

            events_data = []
            enriched_transactions = []

            # Process each transaction (which now includes receipt data)
            for tx_item in transactions_with_receipts:
                transaction = tx_item.get('transaction', {})
                receipt = tx_item.get('receipt', {})

                # Get the transaction hash from either transaction or receipt
                tx_hash = transaction.get('transaction_hash') or receipt.get('transaction_hash')
                if not tx_hash:
                    print(f"Missing 'transaction_hash' in transaction: {tx_item}")
                    continue

                # Merge receipt data into the transaction
                transaction_with_receipt = {**transaction, **receipt}
                enriched_transactions.append(transaction_with_receipt)

                # Extract and enumerate events from the receipt
                for idx, event in enumerate(receipt.get('events', [])):
                    event_id = f"{tx_hash}_{idx:02d}"
                    event_dict = {
                        "event_id": event_id,
                        "tx_hash": tx_hash,
                        "from_address": event.get("from_address"),
                        "data": json.dumps(event.get("data")),
                        "keys": json.dumps(event.get("keys"))
                    }
                    events_data.append(event_dict)

            # Update the block data with enriched transactions
            block_data['transactions'] = enriched_transactions

            # Create a DataFrame from the events data
            events_df = pd.DataFrame(events_data)

            return block_data, events_df

        except Exception as e:
            print(f"Error processing block data and events for block {block_id}: {e}")

    def get_transactions_by_block(self, block_id, rpc_url):
        method = "starknet_getBlockWithTxs"
        params = [{"block_number": block_id}]
        response = self.send_request(method, params, rpc_url)
        if 'result' in response and response['result']:
            transactions = response['result'].get('transactions', [])
            return response['result'], transactions
        else:
            raise Exception(f"No block data available for block {block_id}. Response: {response}")

    def get_block_with_receipts(self, block_id, rpc_url):
        method = "starknet_getBlockWithReceipts"
        params = [{"block_number": block_id}]
        response = self.send_request(method, params, rpc_url)
        if 'result' in response and response['result']:
            return response['result']
        elif 'error' in response and response['error'].get('code') == -32601:
            # Method not found
            print(f"Method {method} not found on RPC {rpc_url}.")
            return None
        else:
            raise Exception(f"No block data available for block {block_id}. Response: {response}")

    def prep_starknet_data(self, full_block_data, strketh_price):        
        # Extract the required fields for each transaction
        extracted_data = []
        for tx in full_block_data['transactions']:
            if not tx.get('events'):
                continue
            last_event = tx['events'][-1]
            from_address = last_event['from_address']
            gas_token = ''  # Default to empty

            # Parse actual_fee and l1_gas_price as integers from hexadecimal strings
            l1_gas_price_hex = full_block_data.get('l1_gas_price', {}).get('price_in_wei', None)
            l1_gas_price = hex_to_int(l1_gas_price_hex) / 1e18 if l1_gas_price_hex else 0

            l1_data_gas_price_hex = full_block_data.get('l1_data_gas_price', {}).get('price_in_wei', None)
            l1_data_gas_price = hex_to_int(l1_data_gas_price_hex) / 1e18 if l1_data_gas_price_hex else 0

            max_fee_hex = tx.get('max_fee', None)
            max_fee = hex_to_int(max_fee_hex) / 1e18 if max_fee_hex else 0

            actual_fee_hex = tx.get('actual_fee', None)
            actual_fee = hex_to_int(actual_fee_hex) / 1e18 if actual_fee_hex else 0
            
            raw_tx_fee = actual_fee                      

            # Parse execution resources
            execution_resources = tx.get('execution_resources', {})
            l1_gas = execution_resources.get('data_availability', {}).get('l1_gas', 0)
            l1_data_gas = execution_resources.get('data_availability', {}).get('l1_data_gas', 0)

            # Determine the DA mode and corresponding gas
            l1_da_mode = full_block_data.get('l1_da_mode', 'CALLDATA')

            if l1_da_mode == 'CALLDATA':
                data_gas_consumed = l1_gas
                data_gas_price = l1_gas_price
            elif l1_da_mode == 'BLOB':
                data_gas_consumed = l1_data_gas
                data_gas_price = l1_data_gas_price
            else:
                data_gas_consumed = 0
                data_gas_price = 0

            # Calculate gas_used based on both old and new ways
            if l1_da_mode in ['CALLDATA', 'BLOB']:
                # (Starknet v0.13.3+)
                computation_gas_price = l1_gas_price
                gas_used = (actual_fee - (data_gas_consumed * data_gas_price)) / computation_gas_price if computation_gas_price > 0 else 0
            else:
                gas_used = actual_fee / l1_gas_price if l1_gas_price > 0 else 0

            # Adjust actual_fee for STRK if applicable
            if actual_fee > 0:
                if from_address == "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d": ## STRK
                    gas_token = 'STRK'
                    actual_fee *= strketh_price
                elif from_address == "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7": ## ETH
                    pass
                else:
                    raise NotImplementedError(f"Gas token from address {from_address} is not yet supported. See transaction: {tx['transaction_hash']}.")
            else:
                print(f"Skipping transaction with zero actual_fee: {tx['transaction_hash']}")

            # Convert timestamp to datetime and extract date
            block_timestamp = pd.to_datetime(full_block_data['timestamp'], unit='s')
            block_date = block_timestamp.date()

            # Append transaction data
            tx_data = {
                'block_number': full_block_data['block_number'],
                'block_timestamp': block_timestamp,
                'block_date': block_date,
                'tx_hash': tx['transaction_hash'],
                'tx_type': tx['type'],
                'max_fee': max_fee,
                'tx_fee': actual_fee,
                'raw_tx_fee': raw_tx_fee,
                'l1_gas_price': l1_gas_price,
                'gas_used': gas_used,
                'status': tx.get('execution_status', None),
                'from_address': tx.get('sender_address', None),
                'gas_token': gas_token
            }
            extracted_data.append(tx_data)

        # Create a DataFrame from the extracted data
        transactions_df = pd.DataFrame(extracted_data)

        return transactions_df

    def insert_data_into_db(self, df, db_connector, table_name, data_type):
        # Determine the primary key based on the data type
        if data_type == 'transaction':
            primary_key = 'tx_hash'
        elif data_type == 'event':
            primary_key = 'event_id'
        else:
            raise ValueError("Invalid data type specified. Please use 'transaction' or 'event'.")

        # Prepare DataFrame for insertion
        df.drop_duplicates(inplace=True)
        df.set_index(primary_key, inplace=True)
        df.index.name = primary_key

        # Insert data into the database
        print(f"Inserting data into table {table_name}...")
        try:
            db_connector.upsert_table(table_name, df, if_exists='update')
        except Exception as e:
            print(f"Error inserting data into table {table_name}: {e}")
            
    def send_request(self, method, params=[], rpc_url=None, max_retries=5):
        if rpc_url is None:
            raise ValueError("rpc_url must be provided")

        headers = {"Content-Type": "application/json"}
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }

        retries = 0
        base_wait_time = 1  # Initial wait time in seconds

        while retries < max_retries:
            try:
                response = requests.post(rpc_url, headers=headers, json=payload)

                if response.status_code == 200:
                    return response.json()

                # Handle 429 (Too Many Requests)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = (
                        int(retry_after)
                        if retry_after and retry_after.isdigit()
                        else base_wait_time + random.uniform(0, 1)
                    )
                    print(f"429 received. Retrying after {wait_time:.2f} seconds.")
                else:
                    raise ConnectionError(f"Unexpected status code: {response.status_code}")

            except Exception as e:
                print(f"Error in request to {rpc_url}: {e}")

            # Increment retries and apply exponential backoff
            retries += 1
            wait_time = min((2 ** retries) + random.uniform(0, 1), 64)
            print(f"Retrying ({retries}/{max_retries}) in {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        raise Exception(f"Request failed after {max_retries} retries to {rpc_url}")

    def backfill_missing_blocks(self, start_block, end_block, batch_size):
        missing_block_ranges = check_and_record_missing_block_ranges(self.db_connector, self.table_name, start_block, end_block)
        if not missing_block_ranges:
            print("No missing block ranges found.")
            return
        print(f"Found {len(missing_block_ranges)} missing block ranges.")
        print("Backfilling missing blocks")
        print("Missing block ranges:")
        for start, end in missing_block_ranges:
            print(f"{start}-{end}")
        block_range_queue = Queue()
        for start, end in missing_block_ranges:
            self.enqueue_block_ranges(start, end, batch_size, block_range_queue)
        self.manage_threads(block_range_queue)
        
def hex_to_int(input_value):
    if isinstance(input_value, str) and input_value.startswith('0x'):
        return int(input_value, 16)
    elif isinstance(input_value, dict) and 'amount' in input_value and isinstance(input_value['amount'], str):
        return int(input_value['amount'], 16)
    elif input_value is None:
        return 0
    else:
        return 0