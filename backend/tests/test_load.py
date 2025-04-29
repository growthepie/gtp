from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
from src.db_connector import DbConnector
import pandas as pd
import argparse

def test_load_data(chain_name, start_block=None, batch_size=None):
    """
    Test loading data for a specific chain with optional validation.
    
    Args:
        chain_name: Name of the blockchain to test
        start_block: Starting block number (None for 'auto')
        batch_size: Number of blocks to process in each batch
        validate: Whether to validate loaded data
    """
    print(f"Testing data load for {chain_name}")
    
    # Initialize DbConnector
    db_connector = DbConnector()
    
    # Get chain configuration
    active_rpc_configs, config_batch_size = get_chain_config(db_connector, chain_name)
    print(f"{chain_name.upper()}_CONFIG={active_rpc_configs}")
    
    # Use provided batch size or config batch size
    if batch_size is None:
        batch_size = config_batch_size
    
    adapter_params = {
        'rpc': 'local_node',
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }
    
    # Initialize NodeAdapter
    adapter = NodeAdapter(adapter_params, db_connector)
    
    # Set load parameters
    load_params = {
        'block_start': 'auto' if start_block is None else start_block,
        'batch_size': batch_size,
    }
    
    try:
        # Load data
        adapter.extract_raw(load_params)
        
            
    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    finally:
        adapter.log_stats()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test data loading for blockchain')
    parser.add_argument('chain', help='Name of the blockchain to test')
    parser.add_argument('--start-block', type=int, help='Starting block number (default: auto)')
    parser.add_argument('--batch-size', type=int, help='Number of blocks per batch')
    
    args = parser.parse_args()
    
    test_load_data(args.chain, args.start_block, args.batch_size, args.validate) 