from datetime import datetime
import numpy as np
import boto3
import botocore
import pandas as pd
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os
import sys
import random
import time
import ast
from src.new_setup.web3 import Web3CC
from sqlalchemy import text
from src.chain_config import adapter_mapping

# ---------------- Utility Functions ---------------------
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

def load_environment():
    load_dotenv()

    # Postgres details from .env file
    db_name = os.getenv("DB_DATABASE")
    db_user = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    return db_name, db_user, db_password, db_host, db_port

# ---------------- Connection Functions ------------------
def connect_to_node(rpc_config):
    try:
        return Web3CC(rpc_config)
    except ConnectionError as e:
        print(f"ERROR: failed to connect to the node with config {rpc_config}: {e}")
        raise

def connect_to_s3():
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
    return s3_connection is not None

def s3_file_exists(s3, file_key, bucket_name):
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

# ---------------- Data Processing Functions -------------
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
    filtered_df['empty_input'] = filtered_df['empty_input'].apply(lambda x: True if (x == '0x' or x == '' or x == b'\x00' or x == b'') else False)

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

def prep_dataframe_opchain(df):
    # Ensure the required columns exist, filling with 0 if they don't
    required_columns = ['l1GasUsed', 'l1GasPrice', 'l1FeeScalar', 'l1Fee']
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
        'l1Fee': 'l1_fee',
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

    # Apply the safe conversion to the l1_fee column
    filtered_df['l1_fee'] = filtered_df['l1_fee'].apply(safe_float_conversion)
    filtered_df['l1_fee'] = filtered_df['l1_fee'].astype('float64')
    filtered_df['l1_fee'].fillna(0, inplace=True)
    
    # Handle 'l1_fee_scalar'
    filtered_df['l1_fee_scalar'].fillna('0', inplace=True)
    filtered_df['l1_fee_scalar'] = pd.to_numeric(filtered_df['l1_fee_scalar'], errors='coerce')

    # Handle 'l1_gas_used'
    filtered_df['l1_gas_used'] = filtered_df['l1_gas_used'].apply(hex_to_int)
    filtered_df['l1_gas_used'].fillna(0, inplace=True)

    # Calculating the tx_fee
    filtered_df['tx_fee'] = ((filtered_df['gas_price'] * filtered_df['gas_used']) + (filtered_df['l1_fee'])) / 1e18
    
    # Convert the 'l1_gas_price' column to eth
    filtered_df['l1_gas_price'] = filtered_df['l1_gas_price'].astype(float) / 1e18

    # Convert the 'l1_fee' column to eth
    filtered_df['l1_fee'] = filtered_df['l1_fee'].astype(float) / 1e18
    
    # Convert the 'input' column to boolean to indicate if it's empty or not
    filtered_df['empty_input'] = filtered_df['empty_input'].apply(lambda x: True if (x == '0x' or x == '' or x == b'\x00' or x == b'') else False)

    # Convert block_timestamp to datetime
    filtered_df['block_timestamp'] = pd.to_datetime(filtered_df['block_timestamp'], unit='s')

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

def prep_dataframe_scroll(df):
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
        'l1Fee': 'l1_fee',
        'block_timestamp': 'block_timestamp'
    }

    # Filter the dataframe to only include the relevant columns
    filtered_df = df[list(column_mapping.keys())]

    # Rename the columns based on the above mapping
    filtered_df = filtered_df.rename(columns=column_mapping)

    filtered_df['l1_fee'] = filtered_df['l1_fee'].apply(lambda x: int(x, 16) / 1e18 if x.startswith('0x') else float(x) / 1e18)
    
    # Convert columns to numeric if they aren't already
    filtered_df['gas_price'] = pd.to_numeric(filtered_df['gas_price'], errors='coerce')
    filtered_df['gas_used'] = pd.to_numeric(filtered_df['gas_used'], errors='coerce')
    
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used']) / 1e18 + filtered_df['l1_fee']
    
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

def prep_dataframe_linea(df):
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
        'block_timestamp': 'block_timestamp'
    }

    # Filter the dataframe to only include the relevant columns
    filtered_df = df[list(column_mapping.keys())]

    # Rename the columns based on the above mapping
    filtered_df = filtered_df.rename(columns=column_mapping)

    # Convert columns to numeric if they aren't already
    filtered_df['gas_price'] = pd.to_numeric(filtered_df['gas_price'], errors='coerce')
    filtered_df['gas_used'] = pd.to_numeric(filtered_df['gas_used'], errors='coerce')
    
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used'])  / 1e18
    
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

def prep_dataframe_arbitrum(df):
    # Define a mapping of old columns to new columns
    column_mapping = {
        'blockNumber': 'block_number',
        'hash': 'tx_hash',
        'from': 'from_address',
        'to': 'to_address',
        'effectiveGasPrice': 'gas_price',
        'gas': 'gas_limit',
        'gasUsed': 'gas_used',
        'value': 'value',
        'status': 'status',
        'input': 'empty_input',
        'block_timestamp': 'block_timestamp',
        'gasUsedForL1': 'gas_used_l1',
    }

    # Filter the dataframe to only include the relevant columns
    filtered_df = df[list(column_mapping.keys())]

    # Rename the columns based on the above mapping
    filtered_df = filtered_df.rename(columns=column_mapping)

    # Convert columns to numeric if they aren't already
    filtered_df['gas_price'] = pd.to_numeric(filtered_df['gas_price'], errors='coerce')
    filtered_df['gas_used'] = pd.to_numeric(filtered_df['gas_used'], errors='coerce')
    
    filtered_df['gas_used_l1'] = filtered_df['gas_used_l1'].apply(hex_to_int)
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used'])  / 1e18
    
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

    # gas_price column in eth
    filtered_df['gas_price'] = filtered_df['gas_price'].astype(float) / 1e18

    # value column divide by 1e18 to convert to eth
    filtered_df['value'] = filtered_df['value'].astype(float) / 1e18

    return filtered_df 

def prep_dataframe_polygon_zkevm(df):
    # Mapping of input columns to the required dataframe structure
    column_mapping = {
        'blockNumber': 'block_number',
        'transactionHash': 'tx_hash',
        'from': 'from_address',
        'to': 'to_address',
        'gasPrice': 'gas_price',
        'effectiveGasPrice': 'effective_gas_price',
        'gas': 'gas_limit',
        'gasUsed': 'gas_used',
        'value': 'value',
        'status': 'status',
        'input': 'empty_input',
        'block_timestamp': 'block_timestamp',
        'contractAddress': 'receipt_contract_address',
        'type': 'type'
    }

    # Filter the dataframe to only include the relevant columns
    df = df[list(column_mapping.keys())]

    # Rename the columns based on the above mapping
    df = df.rename(columns=column_mapping)
    df['tx_hash'] = df['tx_hash'].apply(lambda x: '\\x' + ast.literal_eval(x).hex() if pd.notnull(x) else None)

    # Convert Integer to BYTEA
    df['type'] = df['type'].apply(lambda x: '\\x' + x.to_bytes(4, byteorder='little', signed=True).hex() if pd.notnull(x) else None)
    
    # Use 'effective_gas_price' if available; otherwise, fallback to 'gas_price'
    df['gas_price'] = df['effective_gas_price'].fillna(df['gas_price'])
    
    # Convert numeric columns to appropriate types
    df['gas_price'] = pd.to_numeric(df['gas_price'], errors='coerce')
    df['gas_used'] = pd.to_numeric(df['gas_used'], errors='coerce')
    
    # Convert 'block_time' to datetime
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='s')

    # Convert 'empty_input' column to boolean
    df['empty_input'] = df['empty_input'].apply(lambda x: True if (x == '0x' or x == '') else False)

    # Convert Ethereum values from Wei to Ether for 'gas_price' and 'value'
    df['gas_price'] = df['gas_price'].astype(float) / 1e18
    df['value'] = df['value'].astype(float) / 1e18

    # 'status' column: convert to expected status values
    df['status'] = df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

    # Handle 'to_address', 'from_address', 'receipt_contract_address' for missing values
    address_columns = ['to_address', 'from_address', 'receipt_contract_address']
    for col in address_columns:
        df[col] = df[col].fillna('')

        if col == 'receipt_contract_address':
            df[col] = df[col].apply(lambda x: None if not x or x.lower() == 'none' or x.lower() == '4e6f6e65' else x)
    
    for col in ['tx_hash', 'to_address', 'from_address', 'receipt_contract_address']:
        if col in df.columns:
            df[col] = df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")      
            
    df['tx_fee'] = df['gas_used'] * df['gas_price']
    df.drop(['effective_gas_price'], axis=1, inplace=True)
    return df

def prep_dataframe_blast(df):

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
        'block_timestamp': 'block_timestamp'
    }

    # Filter the dataframe to only include the relevant columns
    filtered_df = df[list(column_mapping.keys())]

    # Rename the columns based on the above mapping
    filtered_df = filtered_df.rename(columns=column_mapping)

    # Convert columns to numeric if they aren't already
    filtered_df['gas_price'] = pd.to_numeric(filtered_df['gas_price'], errors='coerce')
    filtered_df['gas_used'] = pd.to_numeric(filtered_df['gas_used'], errors='coerce')
    
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used']) / 1e18
    
    
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
def prep_dataframe_eth(df):

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
        'input': 'input_data',
        'block_timestamp': 'block_timestamp',
        'maxFeePerGas': 'max_fee_per_gas',
        'maxPriorityFeePerGas': 'max_priority_fee_per_gas',
        'type': 'tx_type',
        'nonce': 'nonce',
        'transactionIndex': 'position',
        'baseFeePerGas': 'base_fee_per_gas',
        'maxFeePerBlobGas': 'max_fee_per_blob_gas'
    }

    # Filter the dataframe to only include the relevant columns that exist in the DataFrame
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    filtered_df = df[existing_columns]

    # Rename the columns based on the above mapping
    existing_column_mapping = {key: column_mapping[key] for key in existing_columns}

    # Rename the columns based on the updated mapping
    filtered_df = filtered_df.rename(columns=existing_column_mapping)

    # Ensure numeric columns are handled appropriately
    for col in ['gas_price', 'gas_used', 'value', 'max_fee_per_gas', 'max_priority_fee_per_gas']:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
    
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used']) / 1e18

    filtered_df['max_fee_per_gas'] = filtered_df['max_fee_per_gas'].astype(float) / 1e18
    filtered_df['max_priority_fee_per_gas'] = filtered_df['max_priority_fee_per_gas'].astype(float) / 1e18
    filtered_df['base_fee_per_gas'] = filtered_df['base_fee_per_gas'].astype(float) / 1e18
    
    if 'max_fee_per_blob_gas' in filtered_df.columns:
        filtered_df['max_fee_per_blob_gas'] = filtered_df['max_fee_per_blob_gas'].apply(lambda x: int(x, 16) if isinstance(x, str) and x.startswith('0x') else float(x)) / 1e18
        
    # Convert the 'input' column to boolean to indicate if it's empty or not
    filtered_df['empty_input'] = filtered_df['input_data'].apply(lambda x: True if (x == '0x' or x == '') else False)

    # Convert block_timestamp to datetime
    filtered_df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='s')

    # status column: 1 if status is success, 0 if failed else -1
    filtered_df['status'] = filtered_df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

    # replace None in 'to_address' column with empty string
    if 'to_address' in filtered_df.columns:
        filtered_df['to_address'] = filtered_df['to_address'].fillna(np.nan)
        filtered_df['to_address'] = filtered_df['to_address'].replace('None', np.nan)

    # Handle bytea data type
    for col in ['tx_hash', 'to_address', 'from_address', 'input_data']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")             

    # gas_price column in eth
    filtered_df['gas_price'] = filtered_df['gas_price'].astype(float) / 1e18

    # value column divide by 1e18 to convert to eth
    filtered_df['value'] = filtered_df['value'].astype(float) / 1e18

    # calculate priority_fee_per_gas
    if 'max_fee_per_gas' in filtered_df.columns and 'base_fee_per_gas' in filtered_df.columns:
        filtered_df['base_fee_per_gas'] = pd.to_numeric(filtered_df['base_fee_per_gas'], errors='coerce')
        filtered_df['priority_fee_per_gas'] = (filtered_df['max_fee_per_gas'] - filtered_df['base_fee_per_gas'])
    else:
        filtered_df['priority_fee_per_gas'] = np.nan
    
    # drop base_fee_per_gas
    filtered_df.drop('base_fee_per_gas', axis=1, inplace=True)

    return filtered_df

def prep_dataframe_zksync_era(df):

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
        'block_timestamp': 'block_timestamp',
        'type': 'type',
        'contractAddress': 'receipt_contract_address'
    }
    
    # Filter the dataframe to only include the relevant columns that exist in the DataFrame
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    filtered_df = df[existing_columns]

    # Rename the columns based on the above mapping
    existing_column_mapping = {key: column_mapping[key] for key in existing_columns}

    # Rename the columns based on the updated mapping
    filtered_df = filtered_df.rename(columns=existing_column_mapping)

    # Convert columns to numeric if they aren't already
    # Ensure numeric columns are handled appropriately
    for col in ['gas_price', 'gas_used', 'value']:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
    
    # Calculating the tx_fee
    filtered_df['tx_fee'] = (filtered_df['gas_price'] * filtered_df['gas_used']) / 1e18

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
        
    # Convert the 'type' integer values to bytea format before insertion
    filtered_df['type'] = filtered_df['type'].apply(lambda x: '\\x' + x.to_bytes(4, byteorder='little', signed=True).hex() if pd.notnull(x) else None)

    # Handle 'to_address', 'from_address', 'receipt_contract_address' for missing values
    address_columns = ['to_address', 'from_address', 'receipt_contract_address']
    for col in address_columns:
        filtered_df[col] = filtered_df[col].fillna('')

        if col == 'receipt_contract_address':
            filtered_df[col] = filtered_df[col].apply(lambda x: None if not x or x.lower() == 'none' or x.lower() == '4e6f6e65' else x)
    
    # Handle bytea data type
    for col in ['tx_hash', 'to_address', 'from_address', 'receipt_contract_address']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")             
    
    # gas_price column in eth
    filtered_df['gas_price'] = filtered_df['gas_price'].astype(float) / 1e18
    
    # value column divide by 1e18 to convert to eth
    filtered_df['value'] = filtered_df['value'].astype(float) / 1e18

    return filtered_df
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
    return db_connector is not None

def create_db_engine(db_user, db_password, db_host, db_port, db_name):
    print("Creating database engine...")
    try:
        # create connection to Postgres
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        engine.connect()  # test connection
        return engine
    except exc.SQLAlchemyError as e:
        print("ERROR: connecting to database. Check your database configurations.")
        print(e)
        sys.exit(1)

# ---------------- Data Interaction --------------------
def get_latest_block(w3):
    try:
        return w3.eth.block_number
    except Exception as e:
        print("ERROR: occurred while fetching the latest block:", str(e))
        return None
    
def fetch_block_transaction_details(w3, block):
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
    print(f"...fetching data for blocks {block_start} to {block_end}. RPC: {w3.get_rpc_url()}")
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

    # Check if the file exists in S3
    if s3_file_exists(s3_connection, file_key, bucket_name):
        print(f"...file {file_key} uploaded to S3 bucket {bucket_name}.")
    else:
        print(f"...file {file_key} not found in S3 bucket {bucket_name}.")
        raise Exception(f"File {file_key} not uploaded to S3 bucket {bucket_name}. Stopping execution.")

def fetch_and_process_range(current_start, current_end, chain, w3, table_name, s3_connection, bucket_name, db_connector, rpc_url):
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
            
            if chain == 'linea':
                df_prep = prep_dataframe_linea(df)
            elif chain == 'scroll':
                df_prep = prep_dataframe_scroll(df)
            elif chain == 'arbitrum':
                df_prep = prep_dataframe_arbitrum(df)
            elif chain == 'polygon_zkevm':
                df_prep = prep_dataframe_polygon_zkevm(df)
            elif chain == 'zksync_era':
                df_prep = prep_dataframe_zksync_era(df)
            elif chain == 'ethereum':
                df_prep = prep_dataframe_eth(df)
            elif chain in ['zora', 'base', 'optimism', 'gitcoin_pgn', 'mantle', 'mode', 'blast', 'redstone']:
                df_prep = prep_dataframe_opchain(df)
            else:
                df_prep = prep_dataframe(df)

            df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
            df_prep.set_index('tx_hash', inplace=True)
            df_prep.index.name = 'tx_hash'
            
            try:
                db_connector.upsert_table(table_name, df_prep, if_exists='update')  # Use DbConnector for upserting data
                print(f"...data inserted for blocks {current_start} to {current_end} successfully. Uploaded rows: {df_prep.shape[0]}. RPC: {w3.get_rpc_url()}")
            except Exception as e:
                print(f"ERROR: {rpc_url} - inserting data for blocks {current_start} to {current_end}: {e}")
                raise e
            break  # Break out of the loop on successful execution

        except Exception as e:
            print(f"ERROR: {rpc_url} - processing blocks {current_start} to {current_end}: {e}")
            base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time, rpc_url)
            # Check if elapsed time exceeds 5 minutes
            if elapsed_time >= 300:
                raise MaxWaitTimeExceededException(f"For {rpc_url}: Maximum wait time exceeded for blocks {current_start} to {current_end}")

def save_to_s3(df, chain, s3_connection, bucket_name):
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
    raw_sql = text(
        "SELECT url, workers, max_requests, max_tps "
        "FROM sys_rpc_config "
        "WHERE active = TRUE AND origin_key = :chain_name AND synced = TRUE"
    )

    with db_connector.engine.connect() as connection:
        result = connection.execute(raw_sql, {"chain_name": chain_name})
        rows = result.fetchall()

    config_list = []
    batch_size = 10 # Default batch size
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

    # Retrieve batch_size from adapter_mapping
    for mapping in adapter_mapping:
        if mapping.origin_key == chain_name:
            batch_size = mapping.batch_size
            break

    return config_list, batch_size