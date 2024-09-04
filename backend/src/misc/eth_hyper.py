import hypersync
import pandas as pd
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc, MetaData, Table
import sys
from pangres import upsert
from dotenv import load_dotenv
import os
import asyncio
import time

# Load environment variables
def load_environment():
    load_dotenv()

    # Postgres details from .env file
    db_name = os.getenv("DB_DATABASE")
    db_user = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
   
    print(db_name, db_user, db_host, db_port)
    return db_name, db_user, db_password, db_host, db_port

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
        
def hex_to_int(hex_str):
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None
    
async def get_blocks_and_transactions(start_block, end_block):
    client = hypersync.HypersyncClient(hypersync.ClientConfig())

    query = hypersync.preset_query_blocks_and_transactions(start_block, end_block)

    print(f"..retrieving blocks {start_block} - {end_block}")

    res = await client.get(query)

    print(f"..retrieved {len(res.data.blocks)} blocks and {len(res.data.transactions)} transactions")

    blocks_data = []
    for block in res.data.blocks:
        blocks_data.append({
            "block_number": block.number,
            "block_timestamp": block.timestamp,
            "base_fee_per_gas": block.base_fee_per_gas,
        })
        
    transactions_data = []
    for txn in res.data.transactions:
        transactions_data.append({
            "tx_hash": txn.hash,
            "block_number": txn.block_number,
            "nonce": txn.nonce,
            "position": txn.transaction_index,
            "from_address": txn.from_,
            "to_address": txn.to,
            "status": txn.status,
            "value": txn.value,
            "gas_limit": txn.gas,
            "gas_used": txn.gas_used,
            "gas_price": txn.gas_price,
            "input_data": txn.input,
            "max_fee_per_gas": txn.max_fee_per_gas,
            "max_priority_fee_per_gas": txn.max_priority_fee_per_gas,
            "max_fee_per_blob_gas": txn.max_fee_per_blob_gas,
            "tx_type": txn.kind,
        })

    blocks_df = pd.DataFrame(blocks_data)
    transactions_df = pd.DataFrame(transactions_data)

    df = pd.merge(transactions_df, blocks_df, on="block_number", how="left")

    return df

def prep_dataframe(df):
    # Ensure numeric columns are handled appropriately
    for col in ['gas_price', 'gas_used', 'value', 'max_fee_per_gas', 'max_priority_fee_per_gas', 'base_fee_per_gas']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculating the tx_fee
    df['tx_fee'] = (df['gas_price'] * df['gas_used']) / 1e18

    df['max_fee_per_gas'] = df['max_fee_per_gas'].astype(float) / 1e18
    df['max_priority_fee_per_gas'] = df['max_priority_fee_per_gas'].astype(float) / 1e18
    df['base_fee_per_gas'] = df['base_fee_per_gas'].astype(float) / 1e18
    
    if 'max_fee_per_blob_gas' in df.columns:
        df['max_fee_per_blob_gas'] = df['max_fee_per_blob_gas'].apply(lambda x: hex_to_int(x) if isinstance(x, str) and x.startswith('0x') else float(x) if x is not None else np.nan) / 1e18
        
    # Convert the 'input' column to boolean to indicate if it's empty or not
    df['empty_input'] = df['input_data'].apply(lambda x: True if (x == '0x' or x == '') else False)

    # Convert block_timestamp to datetime
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='s')

    # status column: 1 if status is success, 0 if failed else -1
    df['status'] = df['status'].apply(lambda x: 1 if x == 1 else 0 if x == 0 else -1)

    # replace None in 'to_address' column with empty string
    if 'to_address' in df.columns:
        df['to_address'] = df['to_address'].fillna(np.nan)
        df['to_address'] = df['to_address'].replace('None', np.nan)

    # Handle bytea data type
    for col in ['tx_hash', 'to_address', 'from_address', 'input_data']:
        if col in df.columns:
            df[col] = df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")             

    ## shorten input_data column to first 4 bytes (only method that is called)
    df['input_data'] = df['input_data'].apply(lambda x: x[:10] if x else None) 
    
    # gas_price column in eth
    df['gas_price'] = df['gas_price'].astype(float) / 1e18

    # value column divide by 1e18 to convert to eth
    df['value'] = df['value'].astype(float) / 1e18

    # calculate priority_fee_per_gas
    if 'max_fee_per_gas' in df.columns and 'base_fee_per_gas' in df.columns:
        df['base_fee_per_gas'] = pd.to_numeric(df['base_fee_per_gas'], errors='coerce')
        df['priority_fee_per_gas'] = (df['max_fee_per_gas'] - df['base_fee_per_gas'])
    else:
        df['priority_fee_per_gas'] = np.nan
    
    # drop base_fee_per_gas
    df.drop('base_fee_per_gas', axis=1, inplace=True)

    return df

def insert_into_db(df, engine, table_name):
    # Set the DataFrame's index to 'tx_hash' (your primary key)
    df.set_index('tx_hash', inplace=True)
    df.index.name = 'tx_hash'
    
    # Insert data into database
    print(f"..inserting data into table {table_name}")
    try:
        upsert(engine, df, table_name, if_row_exists='update')
        print("..data inserted successfully")
    except Exception as e:
        print(f"ERROR: inserting data into table {table_name}.")
        print(e)
        
async def process_blocks_in_batches(start_block, end_block, engine, table_name):
    
    current_block = start_block

    while current_block <= end_block:
        start_time = time.time()
        df = await get_blocks_and_transactions(current_block, end_block)

        if df.empty:
            print("No data found.")
            break

        min_block = df['block_number'].min()
        max_block = df['block_number'].max()
        blocks_processed = max_block - min_block + 1

        # Convert hex values to integers
        cols_to_convert = ['nonce', 'value', 'gas_limit', 'gas_used', 'gas_price', 
                           'block_timestamp', 'max_fee_per_gas', 'max_prio_fee_per_gas', 'base_fee_per_gas']

        for col in cols_to_convert:
            if col in df.columns:
                df[col] = df[col].apply(hex_to_int)

        df = prep_dataframe(df)
        insert_into_db(df, engine, table_name)
        
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"..processed {blocks_processed} blocks from {min_block} to {max_block}. Time passed: {elapsed_time:.4f}s")
        
        current_block = max_block - 1
        

async def main():
    db_name, db_user, db_password, db_host, db_port = load_environment()

    engine = create_db_engine(db_user, db_password, db_host, db_port, db_name)

    table_name = 'ethereum_tx'

    #start_block = 11846709 #original start
    start_block = 12099928 #updated Nader 4th of September
    end_block = 18908594

    # Process blocks in batches and insert into database
    await process_blocks_in_batches(start_block, end_block, engine, table_name)

if __name__ == "__main__":
    asyncio.run(main())
