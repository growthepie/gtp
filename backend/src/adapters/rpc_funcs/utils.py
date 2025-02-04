import ast
from datetime import datetime
import numpy as np
import boto3
import botocore
import pandas as pd
import os
import random
import json
import time
from src.adapters.rpc_funcs.web3 import Web3CC
from sqlalchemy import text
from src.main_config import get_main_config 
from src.adapters.rpc_funcs.chain_configs import chain_configs

# ---------------- Utility Functions ---------------------
def safe_float_conversion(x):
    """
    Safely converts the input value to a float. If the input is a hexadecimal string, 
    it is first converted to an integer before converting to float.
    
    Args:
        x: The input value to convert (can be string, int, etc.).

    Returns:
        float: The converted float value or NaN if the conversion fails.
    """
    try:
        if isinstance(x, str) and x.startswith('0x'):
            return float(int(x, 16))
        return float(x)
    except (ValueError, TypeError):
        return np.nan

def hex_to_int(hex_str):
    """
    Converts a hexadecimal string to an integer.
    
    Args:
        hex_str (str): The hexadecimal string to convert.

    Returns:
        int: The integer value or None if conversion fails.
    """
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None

def convert_input_to_boolean(df):
    """
    Converts 'input_data' or 'empty_input' columns in the DataFrame to boolean values based on the presence of data.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'input_data' or 'empty_input' column.

    Returns:
        pd.DataFrame: DataFrame with the converted boolean values.
    """
    if 'input_data' in df.columns:
        df['empty_input'] = df['input_data'].apply(
            lambda x: True if x in ['0x', '', b'\x00', b''] else False
        ).astype(bool)
    elif 'empty_input' in df.columns:
        df['empty_input'] = df['empty_input'].apply(
            lambda x: True if x in ['0x', '', b'\x00', b''] else False
        ).astype(bool)
    return df

def handle_l1_gas_price(df):
    """
    Safely converts the 'l1_gas_price' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_gas_price' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_gas_price' converted to float.
    """
    if 'l1_gas_price' in df.columns:
        df['l1_gas_price'] = df['l1_gas_price'].apply(safe_float_conversion)
        df['l1_gas_price'] = df['l1_gas_price'].astype('float64')
        df['l1_gas_price'].fillna(0, inplace=True)
    return df

def handle_l1_fee(df):
    """
    Safely converts the 'l1_fee' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_fee' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee' converted to float.
    """
    if 'l1_fee' in df.columns:
        df['l1_fee'] = df['l1_fee'].apply(safe_float_conversion)
        df['l1_fee'] = df['l1_fee'].astype('float64')
        df['l1_fee'].fillna(0, inplace=True)
    
    return df

def handle_l1_blob_base_fee(df):
    """
    Safely converts the 'handle_l1_blob_base_fee' column to float and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'handle_l1_blob_base_fee' column.

    Returns:
        pd.DataFrame: DataFrame with 'handle_l1_blob_base_fee' converted to float.
    """
    if 'l1_blob_base_fee' in df.columns:
        df['l1_blob_base_fee'] = df['l1_blob_base_fee'].apply(safe_float_conversion)
        df['l1_blob_base_fee'] = df['l1_blob_base_fee'].astype('float64')
        df['l1_blob_base_fee'].fillna(0, inplace=True)
    
    return df

def handle_l1_fee_scalar(df):
    """
    Fills NaN values in the 'l1_fee_scalar' column with '0'.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee_scalar' column processed.
    """
    if 'l1_fee_scalar' in df.columns:
        df['l1_fee_scalar'].fillna('0', inplace=True)
    return df

def handle_l1_gas_used(df):
    """
    Converts the 'l1_gas_used' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_gas_used' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_gas_used' column processed.
    """
    if 'l1_gas_used' in df.columns:
        df['l1_gas_used'] = df['l1_gas_used'].apply(hex_to_int)
        df['l1_gas_used'].fillna(0, inplace=True)
    return df

def handle_l1_base_fee_scalar(df):
    """
    Converts the 'l1_base_fee_scalar' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_base_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_base_fee_scalar' column processed.
    """
    if 'l1_base_fee_scalar' in df.columns:
        df['l1_base_fee_scalar'] = df['l1_base_fee_scalar'].apply(hex_to_int)
        df['l1_base_fee_scalar'].fillna(0, inplace=True)
    return df

def handle_l1_blob_base_fee_scalar(df):
    """
    Converts the 'l1_blob_base_fee_scalar' column from hexadecimal to integer and fills NaN values with 0.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'l1_blob_base_fee_scalar' column.

    Returns:
        pd.DataFrame: DataFrame with 'l1_blob_base_fee_scalar' column processed.
    """
    if 'l1_blob_base_fee_scalar' in df.columns:
        df['l1_blob_base_fee_scalar'] = df['l1_blob_base_fee_scalar'].apply(hex_to_int)
        df['l1_blob_base_fee_scalar'].fillna(0, inplace=True)
    return df

def calculate_tx_fee(df):
    """
    Calculates the transaction fee based on gas price, gas used, and L1 fee if applicable.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data.

    Returns:
        pd.DataFrame: DataFrame with a new 'tx_fee' column added.
    """
    if all(col in df.columns for col in ['gas_price', 'gas_used', 'l1_fee']):
        # OpChains calculation
        df['tx_fee'] = (
            (df['gas_price'] * df['gas_used']) + df['l1_fee']
        ) / 1e18
    elif all(col in df.columns for col in ['gas_price', 'gas_used', 'l1_gas_used', 'l1_gas_price', 'l1_fee_scalar']):
        # Default calculation
        df['tx_fee'] = (
            (df['gas_price'] * df['gas_used']) +
            (df['l1_gas_used'] * df['l1_gas_price'] * df['l1_fee_scalar'])
        ) / 1e18
    elif all(col in df.columns for col in ['gas_price', 'gas_used']):
        # Simple calculation
        df['tx_fee'] = (df['gas_price'] * df['gas_used']) / 1e18
    else:
        df['tx_fee'] = np.nan
    return df

def handle_tx_hash(df, column_name='tx_hash'):
    """
    Converts the 'tx_hash' column values into the proper '\\x' hex format if applicable.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'tx_hash' column.
        column_name (str): The name of the column to process (default is 'tx_hash').

    Returns:
        pd.DataFrame: DataFrame with 'tx_hash' column processed.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda tx_hash: '\\x' + (
                tx_hash[2:] if isinstance(tx_hash, str) and tx_hash.startswith('0x') 
                else tx_hash.hex()[2:] if isinstance(tx_hash, bytes) 
                else tx_hash.hex() if isinstance(tx_hash, bytes) 
                else tx_hash
            ) if pd.notnull(tx_hash) else None
        )
    return df

def handle_tx_hash_polygon_zkevm(df, column_name='tx_hash'):
    """
    Processes 'tx_hash' values for the Polygon zkEVM chain by converting them to '\\x' hex format.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'tx_hash' column.
        column_name (str): The name of the column to process (default is 'tx_hash').

    Returns:
        pd.DataFrame: DataFrame with 'tx_hash' values processed for Polygon zkEVM.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda x: '\\x' + ast.literal_eval(x).hex() if pd.notnull(x) else None
        )
    return df

def handle_bytea_columns(df, bytea_columns):
    """
    Processes columns in a DataFrame to handle PostgreSQL bytea types by ensuring proper formatting.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        bytea_columns (list): List of column names that need to be formatted as bytea.

    Returns:
        pd.DataFrame: DataFrame with bytea columns processed.
    """
    for col in bytea_columns:
        if col in df.columns:
            df[col] = df[col].replace(['nan', 'None', 'NaN'], np.nan)
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else x)
            df[col] = df[col].apply(lambda x: x.replace('0x', '\\x') if pd.notna(x) else x)

    return df

def handle_status(df, status_mapping):
    """
    Maps the 'status' column in the DataFrame to corresponding values using a status mapping dictionary.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'status' column.
        status_mapping (dict): Dictionary mapping status values.

    Returns:
        pd.DataFrame: DataFrame with 'status' column processed.
    """
    if 'status' in df.columns:
        default_value = status_mapping.get("default", -1)
        df['status'] = df['status'].apply(lambda x: status_mapping.get(str(x), default_value))
    return df

def handle_address_columns(df, address_columns):
    """
    Processes specified address columns, replacing missing values and handling specific cases like contract addresses.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing address columns.
        address_columns (list): List of column names to process.

    Returns:
        pd.DataFrame: DataFrame with address columns processed.
    """
    for col in address_columns:
        if col in df.columns:
            df[col] = df[col].replace('None', np.nan).fillna('')

            if col == 'receipt_contract_address':
                df[col] = df[col].apply(lambda x: None if not x or x.lower() == 'none' or x.lower() == '4e6f6e65' else x)

            if col == 'to_address':
                df[col] = df[col].replace('', np.nan)
                
    return df

def handle_effective_gas_price(df):
    """
    Replaces 'gas_price' with 'effective_gas_price' where available, and drops the 'effective_gas_price' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing gas price columns.

    Returns:
        pd.DataFrame: DataFrame with effective gas prices handled.
    """
    if 'effective_gas_price' in df.columns and 'gas_price' in df.columns:
        df['gas_price'] = df['effective_gas_price'].fillna(df['gas_price'])
        df.drop(['effective_gas_price'], axis=1, inplace=True)
    return df

def convert_columns_to_numeric(df, numeric_columns):
    """
    Converts specified columns in a DataFrame to numeric types, handling any errors gracefully.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        numeric_columns (list): List of columns to convert to numeric.

    Returns:
        pd.DataFrame: DataFrame with numeric columns converted.
    """
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def convert_columns_to_eth(df, value_conversion):
    """
    Converts specified columns from their original units to Ether by dividing them with the provided divisor.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        value_conversion (dict): A dictionary with column names as keys and divisors as values.

    Returns:
        pd.DataFrame: DataFrame with values converted to Ether.
    """
    for col, divisor in value_conversion.items():
        if col in df.columns:
            df[col] = df[col].astype(float) / divisor
    return df

def shorten_input_data(df):
    """
    Trims 'input_data' column values to 10 characters for storage optimization.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'input_data' column.

    Returns:
        pd.DataFrame: DataFrame with shortened 'input_data' column.
    """
    if 'input_data' in df.columns:
        df['input_data'] = df['input_data'].apply(lambda x: x[:10] if x else None)
    return df

def convert_type_to_bytea(df):
    """
    Converts the 'type' column values to bytea format for storage in PostgreSQL.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the 'type' column.

    Returns:
        pd.DataFrame: DataFrame with 'type' values converted to bytea.
    """
    if 'type' in df.columns:
        df['type'] = df['type'].apply(
            lambda x: '\\x' + int(x).to_bytes(4, byteorder='little', signed=True).hex() 
            if pd.notnull(x) and isinstance(x, (int, float)) else None
        )
    return df

# Custom operation for Scroll
def handle_l1_fee_scroll(df):
    """
    Custom handler for Scroll chain: converts 'l1_fee' to Ether and calculates total transaction fee.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data for the Scroll chain.

    Returns:
        pd.DataFrame: DataFrame with 'l1_fee' and transaction fees processed.
    """
    if 'l1_fee' in df.columns:
        df['l1_fee'] = df['l1_fee'].apply(
            lambda x: int(x, 16) / 1e18 if isinstance(x, str) and x.startswith('0x') else float(x) / 1e18
        )
    
    if 'gas_price' in df.columns and 'gas_used' in df.columns:
        df['tx_fee'] = (df['gas_price'] * df['gas_used']) / 1e18

    if 'l1_fee' in df.columns and 'tx_fee' in df.columns:
        df['tx_fee'] += df['l1_fee']
    
    return df

def calculate_priority_fee(df):
    """
    Calculates the priority fee per gas by subtracting 'base_fee_per_gas' from 'max_fee_per_gas' and converting to Ether.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing gas price columns.

    Returns:
        pd.DataFrame: DataFrame with 'priority_fee_per_gas' calculated.
    """
    if 'max_fee_per_gas' in df.columns and 'base_fee_per_gas' in df.columns:
        df['priority_fee_per_gas'] = (df['max_fee_per_gas'] - df['base_fee_per_gas']) / 1e18
        df.drop('base_fee_per_gas', axis=1, inplace=True)
    else:
        df['priority_fee_per_gas'] = np.nan
    return df

def handle_max_fee_per_blob_gas(df):
    """
    Processes the 'max_fee_per_blob_gas' column by converting its values from hexadecimal to Ether.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing 'max_fee_per_blob_gas' column.

    Returns:
        pd.DataFrame: DataFrame with 'max_fee_per_blob_gas' processed.
    """
    if 'max_fee_per_blob_gas' in df.columns:
        df['max_fee_per_blob_gas'] = df['max_fee_per_blob_gas'].apply(
            lambda x: int(x, 16) / 1e18 if isinstance(x, str) and x.startswith('0x') else float(x) / 1e18
        ).astype(float)
    return df

# ---------------- Connection Functions ------------------
def connect_to_node(rpc_config):
    """
    Establishes a connection to the Ethereum node using the provided RPC configuration.
    
    Args:
        rpc_config (dict): RPC configuration details for connecting to the node.

    Returns:
        Web3CC: The Web3CC object for interacting with the blockchain, or raises a ConnectionError if connection fails.
    """
    try:
        return Web3CC(rpc_config)
    except ConnectionError as e:
        print(f"ERROR: failed to connect to the node with config {rpc_config}: {e}")
        raise

def connect_to_s3():
    """
    Establishes a connection to an S3 bucket using credentials from environment variables.
    
    Returns:
        tuple: A tuple containing the S3 client object and the bucket name.

    Raises:
        ConnectionError: If the connection to S3 fails.
    """
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
        print("ERROR: An error occurred while connecting to S3:", str(e))
        raise ConnectionError(f"An error occurred while connecting to S3: {str(e)}")

def check_s3_connection(s3_connection):
    """
    Checks if the connection to S3 is established.
    
    Args:
        s3_connection: The S3 connection object.

    Returns:
        bool: True if the connection is valid, False otherwise.
    """
    return s3_connection is not None

def s3_file_exists(s3, file_key, bucket_name):
    """
    Checks if a file exists in the specified S3 bucket.
    
    Args:
        s3: The S3 client object.
        file_key (str): The key (file path) in the S3 bucket.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        s3.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            return False
        else:
            raise e

# ---------------- Generic Preparation Function ------------------
def prep_dataframe_new(df, chain):
    """
    Prepares the given DataFrame for storage and processing based on chain-specific configurations.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing transaction data.
        chain (str): The name of the blockchain chain.

    Returns:
        pd.DataFrame: The prepared DataFrame with necessary columns, data types, and operations applied.
    """
    op_chains = ['zora', 'base', 'optimism', 'gitcoin_pgn', 'mantle', 'mode', 'blast', 'redstone', 'orderly', 'derive', 'karak', 'ancient8', 'kroma', 'fraxtal', 'cyber', 'worldchain', 'mint', 'ink', 'soneium', 'swell']
    default_chains = ['manta', 'metis']
    arbitrum_nitro_chains = ['arbitrum', 'gravity', 'real', 'arbitrum_nova']
    
    chain_lower = chain.lower()

    if chain_lower in op_chains:
        config = chain_configs.get('op_chains')
    elif chain_lower in arbitrum_nitro_chains:
        config = chain_configs.get('arbitrum_nitro')
    elif chain_lower in chain_configs:
        config = chain_configs[chain_lower]
    elif chain_lower in default_chains:
        config = chain_configs['default']
    else:
        raise ValueError(f"Chain '{chain}' is not listed in the supported chains.")

    # Ensure the required columns exist, filling with default values
    required_columns = config.get('required_columns', [])
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0

    # Map columns
    column_mapping = config.get('column_mapping', {})
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    df = df[existing_columns]
    df = df.rename(columns=column_mapping)

    # Convert columns to numeric
    numeric_columns = config.get('numeric_columns', [])
    df = convert_columns_to_numeric(df, numeric_columns)

    # Apply special operations
    special_operations = config.get('special_operations', [])
    for operation_name in special_operations:
        operation_function = globals().get(operation_name)
        if operation_function:
            df = operation_function(df)
        else:
            print(f"Warning: Special operation '{operation_name}' not found.")

    # Apply custom operations if any
    custom_operations = config.get('custom_operations', [])
    for op_name in custom_operations:
        operation_function = globals().get(op_name)
        if operation_function:
            df = operation_function(df)
        else:
            print(f"Warning: Custom operation '{op_name}' not found.")

    # Fill NaN values with specified defaults
    fillna_values = config.get('fillna_values', {})
    for col, value in fillna_values.items():
        if col in df.columns:
            df[col].fillna(value, inplace=True)
            
    # Convert date columns
    date_columns = config.get('date_columns', {})
    for col, unit in date_columns.items():
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit=unit)

    # Map status values
    status_mapping = config.get('status_mapping', {})
    df = handle_status(df, status_mapping)

    # Handle address columns
    address_columns = config.get('address_columns', [])
    df = handle_address_columns(df, address_columns)

    # Handle bytea columns
    bytea_columns = config.get('bytea_columns', [])
    df = handle_bytea_columns(df, bytea_columns)

    # Value conversions
    value_conversion = config.get('value_conversion', {})
    df = convert_columns_to_eth(df, value_conversion)

    # Any additional custom steps
    if chain.lower() == 'ethereum':
        df = shorten_input_data(df)

    return df

# ---------------- Error Handling -----------------------
class MaxWaitTimeExceededException(Exception):
    pass

def handle_retry_exception(current_start, current_end, base_wait_time, rpc_url):
    max_wait_time = 60  # Maximum wait time in seconds
    wait_time = min(max_wait_time, 2 * base_wait_time)

    # Check if max_wait_time is reached and raise an exception
    if wait_time >= max_wait_time:
        raise MaxWaitTimeExceededException(f"For {rpc_url}: Maximum wait time exceeded for blocks {current_start} to {current_end}")

    # Add jitter
    jitter = random.uniform(0, wait_time * 0.1)
    wait_time += jitter
    formatted_wait_time = format(wait_time, ".2f")

    print(f"RETRY: for blocks {current_start} to {current_end} after {formatted_wait_time} seconds. RPC: {rpc_url}")
    time.sleep(wait_time)

    return wait_time

# ---------------- Database Interaction ------------------
def check_db_connection(db_connector):
    """
    Checks if the connection to the database is valid.
    
    Args:
        db_connector: The database connector object.

    Returns:
        bool: True if the connection is valid, False otherwise.
    """
    return db_connector is not None

# ---------------- Data Interaction --------------------
def get_latest_block(w3):
    """
    Retrieves the latest block number from the connected Ethereum node.
    
    Args:
        w3: The Web3 instance for interacting with the blockchain.

    Returns:
        int: The latest block number, or None if the retrieval fails after retries.
    """
    retries = 0
    while retries < 3:
        try:
            return w3.eth.block_number
        except Exception as e:
            print("RETRY: occurred while fetching the latest block, but will retry in 3s:", str(e))
            retries += 1
            time.sleep(3)

    print("ERROR: Failed to fetch the latest block after 3 retries.")
    return None
    
def fetch_block_transaction_details(w3, block):
    """
    Fetches detailed information for all transactions in a given block.
    
    Args:
        w3: The Web3 instance for interacting with the blockchain.
        block (dict): The block data containing transactions.

    Returns:
        list: A list of dictionaries containing transaction details.
    """
    transaction_details = []
    block_timestamp = block['timestamp']  # Get the block timestamp
    base_fee_per_gas = block['baseFeePerGas'] if 'baseFeePerGas' in block else None  # Fetch baseFeePerGas from the block

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
        if base_fee_per_gas:
            merged_dict['baseFeePerGas'] = base_fee_per_gas
        
        # Add the transaction receipt dictionary to the list
        transaction_details.append(merged_dict)
        
    return transaction_details
    
def fetch_data_for_range(w3, block_start, block_end):
    """
    Fetches transaction data for a range of blocks and returns it as a DataFrame.
    
    Args:
        w3: The Web3 instance for interacting with the blockchain.
        block_start (int): The starting block number.
        block_end (int): The ending block number.

    Returns:
        pd.DataFrame: A DataFrame containing the transaction data for the specified block range.
    """
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
            print(f"...no transactions found for blocks {block_start} to {block_end}.")
            return None  # Or return an empty df as: return pd.DataFrame()
        else:
            return df

    except Exception as e:
        raise e

def save_data_for_range(df, block_start, block_end, chain, s3_connection, bucket_name):
    """
    Saves the transaction data for a range of blocks to an S3 bucket in parquet format.
    
    Args:
        df (pd.DataFrame): The DataFrame containing transaction data.
        block_start (int): The starting block number.
        block_end (int): The ending block number.
        chain (str): The name of the blockchain chain.
        s3_connection: The S3 connection object.
        bucket_name (str): The name of the S3 bucket.
    """
    # Convert any 'object' dtype columns to string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].apply(str)
            except Exception as e:
                raise e

    # Generate the filename
    filename = f"{chain}_tx_{block_start}_{block_end}.parquet"
    
    # Create S3 file path
    file_key = f"{chain}/{filename}"
    
    # Use the S3 functionality in pandas to write directly to S3
    s3_path = f"s3://{bucket_name}/{file_key}"
    df.to_parquet(s3_path, index=False)

def fetch_and_process_range(current_start, current_end, chain, w3, table_name, s3_connection, bucket_name, db_connector, rpc_url):
    """
    Fetches and processes transaction data for a range of blocks, saves it to S3, and inserts it into the database.
    Retries the operation on failure with exponential backoff.

    Args:
        current_start (int): The starting block number.
        current_end (int): The ending block number.
        chain (str): The name of the blockchain chain.
        w3: The Web3 instance for interacting with the blockchain.
        table_name (str): The database table name to insert data into.
        s3_connection: The S3 connection object.
        bucket_name (str): The name of the S3 bucket.
        db_connector: The database connector object.
        rpc_url (str): The RPC URL used for fetching data.

    Raises:
        MaxWaitTimeExceededException: If the operation exceeds the maximum wait time.
    """
    base_wait_time = 3   # Base wait time in seconds
    start_time = time.time()
    while True:
        try:
            elapsed_time = time.time() - start_time

            df = fetch_data_for_range(w3, current_start, current_end)

            # Check if df is None or empty, and if so, return early without further processing.
            if df is None or df.empty:
                print(f"...skipping blocks {current_start} to {current_end} due to no data.")
                return

            save_data_for_range(df, current_start, current_end, chain, s3_connection, bucket_name)

            df_prep = prep_dataframe_new(df, chain)

            df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
            df_prep.set_index('tx_hash', inplace=True)
            df_prep.index.name = 'tx_hash'

            try:
                db_connector.upsert_table(table_name, df_prep, if_exists='update')  # Use DbConnector for upserting data
                rows_uploaded = df_prep.shape[0]
                print(f"...data inserted for blocks {current_start} to {current_end} successfully. Uploaded rows: {df_prep.shape[0]}. RPC: {w3.get_rpc_url()}")
                return rows_uploaded  # Return the number of rows uploaded
            except Exception as e:
                print(f"ERROR: {rpc_url} - inserting data for blocks {current_start} to {current_end}: {e}")
                raise e

        except Exception as e:
            print(f"ERROR: {rpc_url} - processing blocks {current_start} to {current_end}: {e}")
            base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time, rpc_url)
            # Check if elapsed time exceeds 5 minutes
            if elapsed_time >= 300:
                raise MaxWaitTimeExceededException(f"For {rpc_url}: Maximum wait time exceeded for blocks {current_start} to {current_end}")

def save_to_s3(df, chain, s3_connection, bucket_name):
    """
    Saves the provided DataFrame to an S3 bucket in parquet format.
    
    Args:
        df (pd.DataFrame): The DataFrame to save.
        chain (str): The name of the blockchain chain.
        s3_connection: The S3 connection object.
        bucket_name (str): The name of the S3 bucket.

    Raises:
        Exception: If the file upload fails.
    """
    # Convert any 'object' dtype columns to string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].apply(str)
            except Exception as e:
                print(f"ERROR: converting column {col} to string: {e}")
                raise e
    
    # Generate a unique filename based on the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"{chain}_data_{timestamp}.parquet"
    
    # Create S3 file path
    file_key = f"{chain}/{filename}"
    
    # Use the S3 functionality in pandas to write directly to S3
    s3_path = f"s3://{bucket_name}/{file_key}"
    df.to_parquet(s3_path, index=False)
    
    if s3_file_exists(s3_connection, file_key, bucket_name):
        print(f"...file {file_key} uploaded to S3 bucket {bucket_name}.")
    else:
        print(f"...file {file_key} not found in S3 bucket {bucket_name}.")
        raise Exception(f"File {file_key} not uploaded to S3 bucket {bucket_name}. Stopping execution.")

def get_chain_config(db_connector, chain_name):
    """
    Retrieves the RPC configuration and batch size for the specified blockchain chain.
    
    Args:
        db_connector: The database connector object.
        chain_name (str): The name of the blockchain chain.

    Returns:
        tuple: A tuple containing the list of RPC configurations and the batch size.
    """
    # Determine the SQL query based on the chain name
    if chain_name.lower() == "celestia" or chain_name.lower() == "starknet":
        raw_sql = text(
            "SELECT url, workers, max_requests, max_tps "
            "FROM sys_rpc_config "
            "WHERE origin_key = :chain_name AND active = TRUE "
        )
    else:
        raw_sql = text(
            "SELECT url, workers, max_requests, max_tps "
            "FROM sys_rpc_config "
            "WHERE active = TRUE AND origin_key = :chain_name AND synced = TRUE"
        )

    with db_connector.engine.connect() as connection:
        result = connection.execute(raw_sql, {"chain_name": chain_name})
        rows = result.fetchall()

    config_list = []

    for row in rows:
        config = {"url": row['url']}
        # Add other keys only if they are not None
        if row['workers'] is not None:
            config['workers'] = row['workers']
        if row['max_requests'] is not None:
            config['max_req'] = row['max_requests']
        if row['max_tps'] is not None:
            config['max_tps'] = row['max_tps']
        
        config_list.append(config)

    # Retrieve batch_size
    batch_size = 10
    main_conf = get_main_config()
    for chain in main_conf:
        if chain.origin_key == chain_name:
            if chain.backfiller_batch_size > 0:
                batch_size = chain.backfiller_batch_size
            break

    return config_list, batch_size