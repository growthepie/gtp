import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.rpc_funcs.utils import Web3CC, fetch_block_transaction_details, prep_dataframe_new
import pandas as pd
import time

def test_receipts_fetch_methods():
    """
    Test both methods of fetching transaction receipts:
    1. Using eth_getBlockReceipts batch method
    2. Using individual receipt fetching as fallback
    
    Verify that both methods produce valid data that can be processed correctly.
    """
    print("Starting receipt fetch method test...")
    
    # Use a common RPC endpoint that supports get_block_receipts
    rpc_url = "https://ethereum-rpc.publicnode.com"
    
    # Alternative fallback RPCs in case the main one has issues
    fallback_rpcs = [
        "https://eth.meowrpc.com",
        "https://rpc.mevblocker.io",
        "https://eth-mainnet.public.blastapi.io"
    ]
    
    rpc_config = {'url': rpc_url}
    w3 = None
    
    try:
        print(f"Connecting to {rpc_url}...")
        w3 = Web3CC(rpc_config)
        print(f"Successfully connected to {rpc_url}")
    except Exception as e:
        print(f"Failed to connect to {rpc_url}: {e}")
        
        for fallback_rpc in fallback_rpcs:
            try:
                print(f"Trying fallback RPC: {fallback_rpc}...")
                rpc_config = {'url': fallback_rpc}
                w3 = Web3CC(rpc_config)
                print(f"Successfully connected to {fallback_rpc}")
                break
            except Exception as e:
                print(f"Failed to connect to {fallback_rpc}: {e}")
    
    if w3 is None:
        print("Failed to connect to any RPC endpoint. Test aborted.")
        return
    
    print("Fetching latest block number...")
    latest_block_num = 22265969
    
    test_block_num = latest_block_num - 10
    print(f"Using block {test_block_num} for testing")
    
    print(f"Fetching block {test_block_num}...")
    block = w3.eth.get_block(test_block_num, full_transactions=True)
    
    print("\n=== Testing batch receipts fetch method ===")
    start_time = time.time()
    try:
        save_get_transaction_receipt = w3.eth.get_transaction_receipt
        w3.eth.get_transaction_receipt = lambda _: None  # Simulate failure to force batch method
        transaction_details_batch = fetch_block_transaction_details(w3, block)
        w3.eth.get_transaction_receipt = save_get_transaction_receipt  # Restore function
        
        batch_time = time.time() - start_time
        num_txs_batch = len(transaction_details_batch)
        print(f"Successfully fetched {num_txs_batch} transactions in {batch_time:.2f} seconds using batch method")
        
        df_batch = pd.DataFrame(transaction_details_batch)
        df_batch_processed = prep_dataframe_new(df_batch, "ethereum")
        print("Successfully processed batch-fetched transactions data")
        
    except Exception as e:
        print(f"Error in batch method: {e}")
        import traceback
        traceback.print_exc()
        transaction_details_batch = None
    
    print("\n=== Testing individual receipts fetch method ===")
    start_time = time.time()
    try:
        save_get_block_receipts = w3.eth.get_block_receipts
        w3.eth.get_block_receipts = lambda _: None  # Simulate failure to force individual method
        transaction_details_individual = fetch_block_transaction_details(w3, block)
        w3.eth.get_block_receipts = save_get_block_receipts  # Restore function
        
        individual_time = time.time() - start_time
        num_txs_individual = len(transaction_details_individual)
        print(f"Successfully fetched {num_txs_individual} transactions in {individual_time:.2f} seconds using individual method")
        
        df_individual = pd.DataFrame(transaction_details_individual)
        df_individual_processed = prep_dataframe_new(df_individual, "ethereum")
        print("Successfully processed individually-fetched transactions data")
        
    except Exception as e:
        print(f"Error in individual method: {e}")
        import traceback
        traceback.print_exc()
        transaction_details_individual = None
    
    if transaction_details_batch and transaction_details_individual:
        print("\n=== Comparing results ===")
        batch_cols = sorted(df_batch.columns.tolist())
        individual_cols = sorted(df_individual.columns.tolist())
        
        print(f"Batch method columns: {len(batch_cols)}")
        print(f"Individual method columns: {len(individual_cols)}")
        
        common_cols = set(batch_cols) & set(individual_cols)
        print(f"Common columns: {len(common_cols)}")
        
        if num_txs_batch == num_txs_individual:
            print("Both methods returned the same number of transactions ✓")
        else:
            print(f"WARNING: Different transaction counts: Batch={num_txs_batch}, Individual={num_txs_individual}")
            
    print("\nTest completed!")

if __name__ == "__main__":
    test_receipts_fetch_methods() 