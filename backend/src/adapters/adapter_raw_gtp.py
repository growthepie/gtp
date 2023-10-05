import numpy as np
from web3 import Web3
import boto3
import os
from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.misc.helper_functions import dataframe_to_s3
import botocore
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

class NodeAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("RPC-Raw", adapter_params, db_connector)
        
        self.rpc = adapter_params['rpc']
        self.chain = adapter_params['chain']
        self.url = adapter_params['node_url']
        self.table_name = f'{self.chain}_tx'    
        #self.table_name = f'{self.chain}_tx_nader'    
        # Initialize Web3 connection
        self.w3 = self.connect_to_node()
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = self.connect_to_s3()
                
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.threads = load_params['threads']
        self.run(self.block_start, self.batch_size, self.threads)
        print(f"FINISHED loading raw tx data for {self.chain}.")

    def connect_to_node(self):
        w3 = Web3(HTTPProvider(self.url))
        
        # Apply the geth POA middleware to the Web3 instance
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        if w3.is_connected():
            print("Successfully connected to node.")
            return w3
        else:
            print("Failed to connect to node.")
            raise ConnectionError("Failed to connect to the node.")
        
    def connect_to_s3(self):
        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            bucket_name = os.getenv("S3_LONG_TERM_BUCKET")

            if not aws_access_key_id or not aws_secret_access_key or not bucket_name:
                raise EnvironmentError("AWS access key ID, secret access key, or bucket name not found in environment variables.")

            s3 = boto3.client('s3',
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
            return s3, bucket_name
        except Exception as e:
            print("An error occurred while connecting to S3:", str(e))
            raise ConnectionError(f"An error occurred while connecting to S3: {str(e)}")
    
    def check_s3_connection(self):
        return self.s3_connection is not None

    def check_db_connection(self):
        return self.db_connector is not None
    
    def get_latest_block(self, w3):
        try:
            return w3.eth.block_number
        except Exception as e:
            print("An error occurred while fetching the latest block:", str(e))
            return None
    
    def fetch_data_for_range(self, w3, block_start, block_end):
        print(f"Fetching data for blocks {block_start} to {block_end}...")
        all_transaction_details = []

        try:
            # Loop through each block in the range
            for block_num in range(block_start, block_end + 1):
                block = w3.eth.get_block(block_num, full_transactions=True)  # Added full_transactions=True
                
                # Fetch transaction details for the block using the new function
                transaction_details = fetch_block_transaction_details(w3, block)
                
                all_transaction_details.extend(transaction_details)

            # Convert list of dictionaries to DataFrame
            df = pd.DataFrame(all_transaction_details)

            return df

        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        
    def save_data_for_range(self, df, block_start, block_end):
            # Save the merged dataframe to a parquet file
            filename = f"{self.chain}_tx_{block_start}_{block_end}_nader"
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = df[col].apply(str)
                    except Exception as e:
                        raise e

            df.to_parquet(filename+".parquet")  

            ## upload to S3 under the chain folder
            file_key = f"{self.chain}_nader/{filename}"
            dataframe_to_s3(f'{file_key}', df)
            # Check if the file exists in S3 and delete the local file if it does
            if s3_file_exists(self.s3_connection, self.bucket_name, file_key+".parquet"):
                print(f"File {file_key} uploaded to S3 bucket {self.bucket_name}. Deleting local file...")
                delete_local_file(filename+".parquet")
            else:
                print(f"File {file_key} not found in S3 bucket {self.bucket_name}. Local file not deleted.")
                raise Exception(f"File {file_key} not uploaded to S3 bucket {self.bucket_name}. Stopping execution.")
    
    def prep_dataframe(self, df):
        # Ensure the required columns exist, filling with 0 if they don't
        required_columns = ['l1GasUsed', 'l1GasPrice', 'l1FeeScalar']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0

        # Define a mapping of old columns to new columns
        column_mapping = {
            'blockNumber': 'block_number',
            'hash': 'tx_hash',
            'from': 'from_address',
            'to': 'to_address',
            'gasPrice': 'gas_price',
            'gas': 'gas_limit',
            'gasUsed': 'gas_used',
            'value': 'value',
            'status': 'status',
            'input': 'empty_input',
            'l1GasUsed': 'l1_gas_used',
            'l1GasPrice': 'l1_gas_price',
            'l1FeeScalar': 'l1_fee_scalar',
            'block_timestamp': 'block_timestamp'
        }

        # Filter the dataframe to only include the relevant columns
        filtered_df = df[list(column_mapping.keys())]

        # Rename the columns based on the above mapping
        filtered_df = filtered_df.rename(columns=column_mapping)

        # Convert columns to numeric if they aren't already
        filtered_df['gas_price'] = pd.to_numeric(filtered_df['gas_price'], errors='coerce')
        filtered_df['gas_used'] = pd.to_numeric(filtered_df['gas_used'], errors='coerce')

        # Apply the safe conversion to the l1_gas_price column
        filtered_df['l1_gas_price'] = filtered_df['l1_gas_price'].apply(safe_float_conversion)
        filtered_df['l1_gas_price'] = filtered_df['l1_gas_price'].astype('float64')
        filtered_df['l1_gas_price'].fillna(0, inplace=True)
        
        # Handle 'l1_fee_scalar'
        filtered_df['l1_fee_scalar'].fillna('0', inplace=True)
        filtered_df['l1_fee_scalar'] = pd.to_numeric(filtered_df['l1_fee_scalar'], errors='coerce')

        # Handle 'l1_gas_used'
        filtered_df['l1_gas_used'] = filtered_df['l1_gas_used'].apply(hex_to_int)
        filtered_df['l1_gas_used'].fillna(0, inplace=True)

        # Calculating the tx_fee
        filtered_df['tx_fee'] = ((filtered_df['gas_price'] * filtered_df['gas_used']) + (filtered_df['l1_gas_used'] * filtered_df['l1_gas_price'] * filtered_df['l1_fee_scalar'])) / 1e18
        
        # Convert the 'l1_gas_price' column to eth
        filtered_df['l1_gas_price'] = filtered_df['l1_gas_price'].astype(float) / 1e18
        
        # Convert the 'input' column to boolean to indicate if it's empty or not
        filtered_df['empty_input'] = filtered_df['empty_input'].apply(lambda x: True if (x == '0x' or x == '') else False)

        # Convert block_timestamp to datetime
        filtered_df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='s')

        # status column: 1 if status is success, 0 if failed else -1
        filtered_df['status'] = filtered_df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

        # Handle bytea data type
        for col in ['tx_hash', 'to_address', 'from_address']:
            if col in filtered_df.columns:
                filtered_df[col] = filtered_df[col].str.replace('0x', '\\x', regex=False)
            else:
                print(f"Column {col} not found in dataframe.")
                
        # Convert "0x4E6F6E65" to None in the 'to_address' column
        if 'to_address' in filtered_df.columns:
            filtered_df['to_address'] = filtered_df['to_address'].apply(lambda x: None if x == '\\x4E6F6F6E65' else x)

        # gas_price column in eth
        filtered_df['gas_price'] = filtered_df['gas_price'].astype(float) / 1e18

        # value column divide by 1e18 to convert to eth
        filtered_df['value'] = filtered_df['value'].astype(float) / 1e18

        return filtered_df    
       
    def run(self, block_start, batch_size, threads):
        if not self.check_db_connection():
            raise ConnectionError("Database is not connected.")

        if not self.check_s3_connection():
            raise ConnectionError("S3 is not connected.")

        if not self.w3 or not self.w3.is_connected():
            raise ConnectionError("Not connected to a node.")

        latest_block = self.get_latest_block(self.w3)
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

                futures.append(executor.submit(self.fetch_and_process_range, current_start, current_end))

            # Wait for all threads to complete and handle any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread raised an exception: {e}")

    def fetch_and_process_range(self, current_start, current_end):

        df = self.fetch_data_for_range(self.w3, current_start, current_end)
        self.save_data_for_range(df, current_start, current_end)
        
        df_prep = self.prep_dataframe(df)
        df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
        df_prep.set_index('tx_hash', inplace=True)
        df_prep.index.name = 'tx_hash'
        
        try:
            self.db_connector.upsert_table(self.table_name, df_prep)
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error upserting data to {self.table_name} table. {e}")
            raise e
        
## ----------------- Helper functions --------------------

def fetch_block_transaction_details(w3, block):
    transaction_details = []
    block_timestamp = block['timestamp']  # Get the block timestamp
    
    for tx in block['transactions']:
        tx_hash = tx['hash']
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        # Convert the receipt and transaction to dictionary if it is not
        if not isinstance(receipt, dict):
            receipt = dict(receipt)
        if not isinstance(tx, dict):
            tx = dict(tx)
        
        # Merge transaction and receipt dictionaries
        merged_dict = {**receipt, **tx}
        
        # Add or update specific fields
        merged_dict['hash'] = tx['hash'].hex()
        merged_dict['block_timestamp'] = block_timestamp
        
        # Add the transaction receipt dictionary to the list
        transaction_details.append(merged_dict)
        
    return transaction_details

def delete_local_file(filename):
    try:
        os.remove(filename)
    except Exception as e:
        print(f"Error deleting file {filename}. {e}")

def s3_file_exists(s3, bucket_name, file_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except botocore.exceptions.ClientError as e:
        # If the error code is 404 (Not Found), then the file doesn't exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            return False
        else:
            # Re-raise the exception if it's any other error.
            raise e

def safe_float_conversion(x):
    try:
        if isinstance(x, str) and x.startswith('0x'):
            return float(int(x, 16))
        return float(x)
    except (ValueError, TypeError):
        return np.nan

def hex_to_int(hex_str):
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None 