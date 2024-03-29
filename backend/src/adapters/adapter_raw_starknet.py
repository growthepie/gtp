from src.adapters.funcs_backfill import check_and_record_missing_block_ranges
from src.adapters.abstract_adapters import AbstractAdapterRaw
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from src.adapters.funcs_rps_utils import *
import requests
import pandas as pd
import json

class AdapterStarknet(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("StarkNet", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.url = adapter_params['rpc_url']
        self.table_name = f'{self.chain}_tx'
        self.db_connector = db_connector
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()

    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.threads = load_params['threads']
        self.run(self.block_start, self.batch_size, self.threads)
        print(f"FINISHED loading raw tx data for {self.chain}.")

    def run(self, block_start, batch_size, threads):
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")

        if not check_s3_connection(self.s3_connection):
            raise ConnectionError("S3 is not connected.")
        else:
            print("Successfully connected to S3.")

        latest_block = self.get_latest_block_id()
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
                current_end = min(current_start + batch_size - 1, latest_block)
                futures.append(executor.submit(self.fetch_and_process_range_starknet, current_start, current_end, self.chain, self.s3_connection, self.bucket_name, self.db_connector))
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread raised an exception: {e}")
                    traceback.print_exc()

    def get_latest_block_id(self):
        method = "starknet_blockNumber"
        response = self.send_request(method)
        if 'result' in response:
            return response['result']
        else:
            raise Exception("Failed to retrieve the latest block number.")

    def get_transactions_with_receipts(self, block_id):
        try:
            block_data, transactions = self.get_transactions_by_block(block_id)

            for transaction in transactions:
                tx_hash = transaction['transaction_hash']
                receipt = self.get_transaction_receipt(tx_hash)

                # Merge receipt data into the transaction
                for key, value in receipt.items():
                    if key not in transaction:
                        transaction[key] = value

            block_data['transactions'] = transactions
            return block_data
        except Exception as e:
            raise Exception(f"Error enriching transactions with receipts: {str(e)}")
    
    def get_transaction_receipt(self, tx_hash):
        method = "starknet_getTransactionReceipt"
        params = [tx_hash]
        response = self.send_request(method, params)
        if 'result' in response:
            return response['result']
        else:
            raise Exception(f"No receipt data available for transaction {tx_hash}. Response: {response}")

    def get_transactions_by_block(self, block_id):
        method = "starknet_getBlockWithTxs"
        params = [{"block_number": block_id}]
        response = self.send_request(method, params)
        if 'result' in response and response['result']:
            transactions = response['result'].get('transactions', [])
            return response['result'], transactions
        else:
            raise Exception(f"No block data available for block {block_id}. Response: {response}")

    def get_block_data_and_events(self, block_id):
        try:
            block_data, transactions = self.get_transactions_by_block(block_id)
            if not transactions:
                print(f"No transactions found for block {block_id}.")
                return None, pd.DataFrame()
            
            events_data = []

            # Process each transaction
            for transaction in transactions:
                tx_hash = transaction.get('transaction_hash')
                if not tx_hash:
                    print(f"Missing 'transaction_hash' in transaction: {transaction}")
                    continue  # Skip this transaction if 'transaction_hash' is missing
                try:
                    receipt = self.get_transaction_receipt(tx_hash)
                    if not receipt:
                        print(f"No receipt found for transaction hash: {tx_hash}")
                        continue
                    
                    # Merge receipt data into the transaction
                    for key, value in receipt.items():
                        if key not in transaction:
                            transaction[key] = value

                    # Extract and enumerate events from the receipt for the events DataFrame
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
                        
                except Exception as e:
                    print(f"Unexpected error when processing transaction {tx_hash}: {e}")
                    
            # Update the block data with enriched transactions
            block_data['transactions'] = transactions

            # Create a DataFrame from the events data
            events_df = pd.DataFrame(events_data)

            return block_data, events_df
        
        except Exception as e:
            print(f"Error processing block data and events for block {block_id}: {e}")
       
    def fetch_and_process_range_starknet(self, current_start, current_end, chain, s3_connection, bucket_name, db_connector):
        base_wait_time = 5
        while True:
            try:
                transactions_df, events_df = self.fetch_starknet_data_for_range(current_start, current_end)
                
                # Check if both DataFrames are empty
                if transactions_df.empty and events_df.empty:
                    print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                    return

                # Process and save transactions
                if not transactions_df.empty:
                    save_data_for_range(transactions_df, current_start, current_end, chain, s3_connection, bucket_name)
                    self.insert_data_into_db(transactions_df, db_connector, 'starknet_tx', 'transaction')
                
                # Process and save events
                if not events_df.empty:
                    self.insert_data_into_db(events_df, db_connector, 'starknet_events', 'event')
                
                print(f"Data inserted for blocks {current_start} to {current_end} successfully.")
                break

            except Exception as e:
                print(f"Error processing blocks {current_start} to {current_end}: {e}")
                base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time)

    def fetch_starknet_data_for_range(self, current_start, current_end):
        print(f"Fetching data for blocks {current_start} to {current_end}...")
        all_blocks_dfs = []
        all_events_dfs = []
        strketh_price = self.db_connector.get_last_price_eth('starknet')
        print(f"pulled latest STRK/ETH price: {strketh_price}")

        for block_id in range(current_start, current_end + 1):
            try:
                full_block_data, events_df = self.get_block_data_and_events(block_id)
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


    def prep_starknet_data(self, full_block_data, strketh_price):        
        # Extract the required fields for each transaction
        extracted_data = []
        for tx in full_block_data['transactions']:
            last_event = tx['events'][-1]
            from_address = last_event['from_address']
            gas_token = ''  # Default to empty

            # Parse actual_fee and l1_gas_price as integers from hexadecimal strings
            l1_gas_price_hex = full_block_data.get('l1_gas_price', {}).get('price_in_wei', None)
            l1_gas_price = hex_to_int(l1_gas_price_hex) / 1e18

            max_fee_hex = tx.get('max_fee', None)
            max_fee = hex_to_int(max_fee_hex) / 1e18

            actual_fee_hex = tx.get('actual_fee', None)
            actual_fee = hex_to_int(actual_fee_hex) / 1e18
            
            raw_tx_fee = actual_fee                      

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

            gas_used = actual_fee // l1_gas_price if l1_gas_price != 0 else 0
                
            tx_data = {
                'block_number': full_block_data['block_number'],
                'block_timestamp': pd.to_datetime(full_block_data['timestamp'], unit='s'),
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
            
# Helper Function
    def send_request(self, method, params=[], rpc_url=None):
        if rpc_url is None:
            rpc_url = self.url

        headers = {"Content-Type": "application/json"}
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        response = requests.post(rpc_url, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
        return response.json()

    def process_missing_blocks_in_batches(self, missing_block_ranges, batch_size, threads):
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []

            for start_block, end_block in missing_block_ranges:
                for batch_start in range(start_block, end_block + 1, batch_size):
                    batch_end = min(batch_start + batch_size - 1, end_block)
                    future = executor.submit(self.fetch_and_process_range_starknet, batch_start, batch_end, self.chain, self.s3_connection, self.bucket_name, self.db_connector)
                    futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"An error occurred during backfilling: {e}")

    def backfill_missing_blocks(self, start_block, end_block, batch_size, threads):
        missing_block_ranges = check_and_record_missing_block_ranges(self.db_connector, self.table_name, start_block, end_block)
        self.process_missing_blocks_in_batches(missing_block_ranges, batch_size, threads)
        
def hex_to_int(input_value):
    if isinstance(input_value, str) and input_value.startswith('0x'):
        return int(input_value, 16)
    elif isinstance(input_value, dict) and 'amount' in input_value and isinstance(input_value['amount'], str):
        return int(input_value['amount'], 16)
    elif input_value is None:
        # Handle None input by returning 0 or another default value
        return 0
    else:
        print(f"Unexpected input type or format for hex_to_int: {type(input_value)} with value {input_value}")
        return 0
