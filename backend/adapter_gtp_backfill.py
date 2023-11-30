import os
from web3 import Web3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc
import sys
import random
import time
import boto3
import numpy as np
from pangres import upsert
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3, HTTPProvider
import botocore
from web3.middleware import geth_poa_middleware
import json
from sqlalchemy import text

# Constants
CHAIN = "mantle"
BATCH_SIZE = 150
THREADS = 7
TABLE_NAME=CHAIN + "_tx"
MISSING_BLOCKS_FILE= "missing_blocks_" + TABLE_NAME + ".json"

# Load environment variables
load_dotenv()
RPC_URL = os.getenv(CHAIN.upper() + "_RPC")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME=os.getenv("S3_LONG_TERM_BUCKET")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ------------------ Helper Functions ------------------
def safe_float_conversion(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return np.nan 

def hex_to_int(hex_str):
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None 

# ------------------ AWS S3 Functions ------------------
def connect_to_s3():
    try:
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not BUCKET_NAME:
            print("AWS access key ID, secret access key, or bucket name not found in environment variables.")
            raise EnvironmentError("AWS access key ID, secret access key, or bucket name not found in environment variables.")

        s3 = boto3.client('s3',
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        return s3
    except Exception as e:
        print("An error occurred while connecting to S3:", str(e))
        raise ConnectionError(f"An error occurred while connecting to S3: {str(e)}")
    
def s3_file_exists(s3, file_key):
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
        return True
    except botocore.exceptions.ClientError as e:
        # If the error code is 404 (Not Found), then the file doesn't exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            return False
        else:
            # Re-raise the exception if it's any other error.
            raise e

def save_data_for_range(df, s3_connection, block_start, block_end):
    # Convert any 'object' dtype columns to string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].apply(str)
            except Exception as e:
                raise e

    # Generate the filename
    filename = f"{CHAIN}_tx_{block_start}_{block_end}.parquet"
    
    # Create S3 file path
    file_key = f"{CHAIN}/{filename}"
    
    # Use the S3 functionality in pandas to write directly to S3
    s3_path = f"s3://{BUCKET_NAME}/{file_key}"
    df.to_parquet(s3_path, index=False)

    # Check if the file exists in S3
    if s3_file_exists(s3_connection, file_key):
        print(f"File {file_key} uploaded to S3 bucket {BUCKET_NAME}.")
    else:
        print(f"File {file_key} not found in S3 bucket {BUCKET_NAME}.")
        raise Exception(f"File {file_key} not uploaded to S3 bucket {BUCKET_NAME}. Stopping execution.")

# ------------------ Database Functions ------------------
def upsert_table( df:pd.DataFrame, engine, table_name:str, if_exists='update'):
        batch_size = 100000
        if df.shape[0] > 0:
                if df.shape[0] > batch_size:
                        print(f"Batch upload necessary. Total size: {df.shape[0]}")
                        total_length = df.shape[0]
                        batch_start = 0
                        while batch_start < total_length:
                                batch_end = batch_start + batch_size
                                upsert(con=engine, df=df.iloc[batch_start:batch_end], table_name=table_name, if_row_exists=if_exists, create_table=False)
                                print("Batch " + str(batch_end))
                                batch_start = batch_end
                else:
                        upsert(con=engine, df=df, table_name=table_name, if_row_exists='update', create_table=False)
                return df.shape[0] 

def create_db_engine(db_user, db_password, db_host, db_port, db_name):
    print("Creating database engine...")
    try:
        # create connection to Postgres
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        engine.connect()  # test connection
        return engine
    except exc.SQLAlchemyError as e:
        print("Error connecting to database. Check your database configurations.")
        print(e)
        sys.exit(1)

# ------------------ Chain Functions ------------------
def connect_to_node():
    w3 = Web3(HTTPProvider(RPC_URL))
    
    # Apply the geth POA middleware to the Web3 instance
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    
    # Check if connected to Ethereum node
    if w3.is_connected():
        print("Web3 instance is connected to node: {}".format(w3.is_connected()))
        return w3
    else:
        print("Failed to connect to Ethereum node.")
        return None
      
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

def fetch_data_for_range(w3, block_start, block_end):
    print(f"Fetching data for blocks {block_start} to {block_end}...")
    all_transaction_details = []

    try:
        # Loop through each block in the range
        for block_num in range(block_start, block_end + 1):
            block = w3.eth.get_block(block_num, full_transactions=True)
            
            # Fetch transaction details for the block using the new function
            transaction_details = fetch_block_transaction_details(w3, block)
            
            all_transaction_details.extend(transaction_details)

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(all_transaction_details)
        
        # if df doesn't have any records, then handle it gracefully
        if df.empty:
            print(f"No transactions found for blocks {block_start} to {block_end}.")
            return None  # Or return an empty df as: return pd.DataFrame()
        else:
            return df

    except Exception as e:
        raise e
    
def prep_dataframe(df):
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

    # replace None in 'to_address' column with empty string
    if 'to_address' in filtered_df.columns:
        filtered_df['to_address'] = filtered_df['to_address'].fillna(np.nan)
        filtered_df['to_address'] = filtered_df['to_address'].replace('None', np.nan)

    # Handle bytea data type
    for col in ['tx_hash', 'to_address', 'from_address']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")             

    # gas_price column in eth
    filtered_df['gas_price'] = filtered_df['gas_price'].astype(float) / 1e18

    # value column divide by 1e18 to convert to eth
    filtered_df['value'] = filtered_df['value'].astype(float) / 1e18

    return filtered_df   

# ------------------ Batch Processing Functions ------------------
def get_actual_tx_count(w3, block_start, block_end):
    actual_tx_counts = {}
    for block_num in range(block_start, block_end + 1):
        block = w3.eth.get_block(block_num)
        actual_tx_counts[block_num] = len(block.transactions)
    return actual_tx_counts

def are_transactions_present(engine, table_name, block_start, block_end, w3):
    actual_tx_counts = get_actual_tx_count(w3, block_start, block_end)

    query = f"""
    SELECT block_number, COUNT(*) as tx_count
    FROM {table_name}
    WHERE block_number BETWEEN {block_start} AND {block_end}
    GROUP BY block_number
    """
    result = engine.execute(query).fetchall()
    db_tx_counts = {row['block_number']: row['tx_count'] for row in result}

    for block_num in range(block_start, block_end + 1):
        if actual_tx_counts.get(block_num, 0) != db_tx_counts.get(block_num, 0):
            return False

    return True

def process_blocks_in_batches_tx(engine, table_name, s3_connection, latest_block, batch_size, w3):
    for start_block in range(0, latest_block + 1, batch_size):
        end_block = min(start_block + batch_size - 1, latest_block)

        # Check if all transactions are present in this range
        if are_transactions_present(engine, table_name, start_block, end_block, w3):
            print(f"All transactions are in DB for range {start_block} to {end_block}.")
        else:
            print(f"Transactions missing in DB for range {start_block} to {end_block}. Fetching and processing data...")
            fetch_and_process_range(w3, s3_connection, engine, start_block, end_block)
           
def fetch_and_process_range(w3, s3_connection, engine, current_start, current_end):
    base_wait_time = 5   # Base wait time in seconds
    while True:
        try:
            df = fetch_data_for_range(w3, current_start, current_end)

            # Check if df is None or empty, and if so, return early without further processing.
            if df is None or df.empty:
                print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                return
            
            save_data_for_range(df, s3_connection, current_start, current_end)
            
            df_prep = prep_dataframe(df)

            df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
            df_prep.set_index('tx_hash', inplace=True)
            df_prep.index.name = 'tx_hash'
            
            try:
                upsert_table(df_prep, engine, TABLE_NAME)
                print(f"Data inserted for blocks {current_start} to {current_end} successfully.")
            except Exception as e:
                print(f"Error inserting data for blocks {current_start} to {current_end}: {e}")
                raise e
            break  # Break out of the loop on successful execution

        except Exception as e:
            print(f"Error processing blocks {current_start} to {current_end}: {e}")
            base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time)

def handle_retry_exception(current_start, current_end, base_wait_time):
    max_wait_time = 300  # Maximum wait time in seconds
    wait_time = min(max_wait_time, 2 * base_wait_time)

    # Add jitter
    jitter = random.uniform(0, wait_time * 0.1)
    wait_time += jitter
    formatted_wait_time = format(wait_time, ".2f")

    print(f"Retrying for blocks {current_start} to {current_end} after {formatted_wait_time} seconds.")
    time.sleep(wait_time)

    return wait_time

def check_and_record_missing_block_ranges(engine, table_name, latest_block):

    print(f"Checking and recording missing block ranges for table: {table_name}")

    # Find the smallest block number in the database
    smallest_block_query = text(f"SELECT MIN(block_number) AS min_block FROM {table_name};")
    with engine.connect() as connection:
        result = connection.execute(smallest_block_query).fetchone()
        min_block = result[0] if result[0] is not None else 0  # Access the first item in the tuple

    print(f"Starting check from block number: {min_block}, up to latest block: {latest_block}")

    # Adjusted query to start from the smallest block number and go up to the latest block
    query = text(f"""
        WITH RECURSIVE missing_blocks (block_number) AS (
            SELECT {min_block} AS block_number
            UNION ALL
            SELECT block_number + 1
            FROM missing_blocks
            WHERE block_number < {latest_block}
        )
        SELECT mb.block_number
        FROM missing_blocks mb
        LEFT JOIN {table_name} t ON mb.block_number = t.block_number
        WHERE t.block_number IS NULL
        ORDER BY mb.block_number;
    """)

    with engine.connect() as connection:
        missing_blocks_result = connection.execute(query).fetchall()

    if not missing_blocks_result:
        print(f"No missing block ranges found for table: {table_name}.")
        return

    missing_ranges = []
    start_missing_range = None
    previous_block = None

    for row in missing_blocks_result:
        current_block = row[0]
        if start_missing_range is None:
            start_missing_range = current_block

        if previous_block is not None and current_block != previous_block + 1:
            missing_ranges.append((start_missing_range, previous_block))
            start_missing_range = current_block

        previous_block = current_block

    # Add the last range if it exists
    if start_missing_range is not None:
        missing_ranges.append((start_missing_range, previous_block))

    # Save to JSON file
    with open(MISSING_BLOCKS_FILE, 'w') as file:
        json.dump(missing_ranges, file)

    print(f"Missing block ranges saved to {MISSING_BLOCKS_FILE}")

def process_missing_blocks_in_batches(engine, s3_connection, json_file, batch_size, w3):
    with open(json_file, 'r') as file:
        missing_block_ranges = json.load(file)

    total_ranges = len(missing_block_ranges)
    print(f"Total block ranges to process: {total_ranges}")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        range_counter = total_ranges  # Initialize a counter for the countdown

        for block_range in missing_block_ranges:
            start_block = block_range[0]
            end_block = block_range[1]

            for batch_start in range(start_block, end_block + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_block)
                future = executor.submit(fetch_and_process_range, w3, s3_connection, engine, batch_start, batch_end)
                futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
                range_counter -= 1  # Decrement the counter
                print(f"Processed a block range. Remaining: {range_counter}")
            except Exception as e:
                print(f"An error occurred: {e}")

    # After processing all ranges, delete the JSON file
    try:
        os.remove(json_file)
        print(f"Successfully deleted the file: {json_file}")
    except OSError as e:
        print(f"Error: {e.filename} - {e.strerror}.")

def main():    
    engine = create_db_engine(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    s3_connection = connect_to_s3()
    # Connect to node
    w3 = connect_to_node()
    if w3 is None:
        print("Failed to connect to the Ethereum node.")
        sys.exit(1)

    # Determine the latest block number
    latest_block = w3.eth.block_number
    print(f"Latest block number: {latest_block}")
    
    # Check and record missing block ranges
    check_and_record_missing_block_ranges(engine, TABLE_NAME, latest_block)
    process_missing_blocks_in_batches(engine, s3_connection, MISSING_BLOCKS_FILE, BATCH_SIZE, w3)

if __name__ == "__main__":
    main()