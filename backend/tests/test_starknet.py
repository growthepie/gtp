from src.adapters.adapter_raw_starknet import AdapterStarknet
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
from src.db_connector import DbConnector
import pandas as pd
from datetime import datetime

def verify_block_data(db_connector, block_number):
    """Verify the data for a specific block in the database"""
    query = f"""
    SELECT 
        block_number,
        block_timestamp,
        block_date,
        tx_hash,
        tx_type,
        tx_fee,
        gas_token
    FROM starknet_tx 
    WHERE block_number = {block_number}
    ORDER BY block_timestamp
    """
    
    df = pd.read_sql(query, db_connector.engine)
    print(f"\nVerifying data for block {block_number}:")
    print(f"Number of transactions: {len(df)}")
    if not df.empty:
        print("\nSample transaction data:")
        print(df.head())
        print("\nBlock date verification:")
        print(f"Block timestamp: {df['block_timestamp'].iloc[0]}")
        print(f"Block date: {df['block_date'].iloc[0]}")
        print(f"Expected date: {pd.to_datetime(df['block_timestamp'].iloc[0]).date()}")
        
        # Verify block_date matches the date from block_timestamp
        assert df['block_date'].iloc[0] == pd.to_datetime(df['block_timestamp'].iloc[0]).date(), \
            f"Block date {df['block_date'].iloc[0]} does not match timestamp date {pd.to_datetime(df['block_timestamp'].iloc[0]).date()}"
    return df

def run_starknet():
    # Initialize DbConnector
    db_connector = DbConnector()
    
    chain_name = 'starknet'
    start_block = 408816
    batch_size = 1

    print(f"\nStarting StarkNet test for blocks {start_block} to {start_block + batch_size - 1}")

    active_rpc_configs, _ = get_chain_config(db_connector, chain_name)
    print(f"STARKNET_CONFIG={active_rpc_configs}")

    adapter_params = {
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }

    # Initialize AdapterStarknet
    adapter = AdapterStarknet(adapter_params, db_connector)

    # Initial load parameters
    load_params = {
        'block_start': start_block,
        'batch_size': batch_size,
    }

    try:
        # Run the extraction
        print("\nRunning data extraction...")
        adapter.extract_raw(load_params)
        print("Extraction completed successfully")

        # Verify data for each block
        for block_num in range(start_block, start_block + batch_size):
            df = verify_block_data(db_connector, block_num)
            if not df.empty:
                print(f"✓ Block {block_num} data verified successfully")
            else:
                print(f"! No transactions found in block {block_num}")

    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    except Exception as e:
        print(f"Error during test: {e}")
        raise e

if __name__ == "__main__":
    run_starknet() 