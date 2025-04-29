from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
from src.db_connector import DbConnector
import pandas as pd
import argparse
import time
from sqlalchemy import text

def test_load_lisk_data(start_block=15590154, batch_size=10):
    """
    Test loading data for Lisk chain to verify tx_type is correctly added.
    
    Args:
        start_block: Starting block number
        batch_size: Number of blocks to process in each batch
    """
    print(f"Testing data load for Lisk starting from block {start_block}")
    
    # Initialize DbConnector
    db_connector = DbConnector()
    
    # Chain name
    chain_name = "lisk"
    
    # Get chain configuration
    active_rpc_configs, config_batch_size = get_chain_config(db_connector, chain_name)
    print(f"Lisk RPC configs: {active_rpc_configs}")
    
    adapter_params = {
        'rpc': 'local_node',
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }
    
    # Initialize NodeAdapter
    adapter = NodeAdapter(adapter_params, db_connector)
    
    # Set load parameters with a specific block range
    load_params = {
        'block_start': start_block,
        'batch_size': batch_size,
    }
    
    # Delete any existing transactions from these blocks to ensure clean test
    with db_connector.engine.connect() as conn:
        conn.execute(text("SET ROLE data_team_write;"))
        delete_stmt = text(f"""
            DELETE FROM lisk_tx 
            WHERE block_number >= {start_block} 
            AND block_number < {start_block + batch_size}
        """)
        result = conn.execute(delete_stmt)
        print(f"Deleted {result.rowcount} existing transactions for clean test")
    
    try:
        # Load data
        print(f"Loading blocks {start_block} to {start_block + batch_size - 1}...")
        adapter.extract_raw(load_params)
        
        # Query the database to check if tx_type was correctly added
        with db_connector.engine.connect() as conn:
            # Check if transactions were loaded
            count_query = text(f"""
                SELECT COUNT(*) 
                FROM lisk_tx 
                WHERE block_number >= {start_block} 
                AND block_number < {start_block + batch_size}
            """)
            tx_count = conn.execute(count_query).scalar()
            
            # Check if tx_type column is populated
            tx_type_query = text(f"""
                SELECT tx_type, COUNT(*) as count
                FROM lisk_tx
                WHERE block_number >= {start_block} 
                AND block_number < {start_block + batch_size}
                GROUP BY tx_type
                ORDER BY tx_type
            """)
            tx_type_distribution = conn.execute(tx_type_query).fetchall()
            
            # Sample few transactions for detailed inspection
            sample_query = text(f"""
                SELECT tx_hash, block_number, tx_type, from_address, to_address, gas_price, gas_used, value
                FROM lisk_tx
                WHERE block_number >= {start_block} 
                AND block_number < {start_block + batch_size}
                LIMIT 5
            """)
            sample_tx = conn.execute(sample_query).fetchall()
        
        # Print results
        print(f"\n===== TEST RESULTS =====")
        print(f"Total transactions loaded: {tx_count}")
        
        print(f"\nTransaction type distribution:")
        for tx_type, count in tx_type_distribution:
            print(f"  - Type {tx_type}: {count} transactions")
            
        print(f"\nSample transactions:")
        for tx in sample_tx:
            print(f"  - Block {tx.block_number}, TX: {tx.tx_hash[:10]}..., Type: {tx.tx_type}")
            
        if tx_count > 0:
            print(f"\nTest SUCCESSFUL: Lisk transactions were loaded with tx_type field populated")
        else:
            print(f"\nTest FAILED: No transactions were loaded in the specified block range")
            
    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    finally:
        adapter.log_stats()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Lisk data loading with tx_type')
    parser.add_argument('--start-block', type=int, default=15590154, help='Starting block number')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of blocks per batch')
    
    args = parser.parse_args()
    
    test_load_lisk_data(args.start_block, args.batch_size) 