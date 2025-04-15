import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.rpc_funcs.utils import Web3CC, get_chain_config
from src.db_connector import DbConnector

def test_batch_receipt_support():
    """
    Tests all the Ethereum RPC endpoints to see which ones support eth_getBlockReceipts.
    """
    print("Testing which Ethereum RPC endpoints support eth_getBlockReceipts...\n")
    
    print("Connecting to database...")
    db_connector = DbConnector()
    print("Successfully connected to database.")
    
    chain_name = 'ethereum'
    rpc_configs, _ = get_chain_config(db_connector, chain_name)
    
    print(f"Found {len(rpc_configs)} RPC endpoints for {chain_name}:")
    for i, config in enumerate(rpc_configs):
        print(f"{i+1}. {config['url']} (workers: {config.get('workers', 'N/A')})")
    
    reference_block_hash = None
    for config in rpc_configs:
        try:
            w3 = Web3CC(config)
            latest_block = w3.eth.block_number
            test_block = latest_block - 10
            block = w3.eth.get_block(test_block)
            reference_block_hash = block['hash']
            print(f"\nUsing block {test_block} (hash: {reference_block_hash.hex()}) for testing")
            break
        except Exception as e:
            print(f"Could not connect to {config['url']}: {e}")
            continue
    
    if reference_block_hash is None:
        print("Could not connect to any RPC endpoint to find a reference block. Aborting.")
        return
    
    results = []
    
    print("\nTesting RPC endpoints for batch receipt support:")
    print("=" * 80)
    print(f"{'#':<3} {'RPC URL':<50} {'Support':<15} {'Notes'}")
    print("-" * 80)
    
    for i, config in enumerate(rpc_configs):
        rpc_url = config['url']
        
        try:
            w3 = Web3CC(config)
            
            # Try to use eth_getBlockReceipts
            try:
                receipts = w3.eth.get_block_receipts(reference_block_hash)
                receipts_count = len(receipts)
                supports_batch = True
                notes = f"Returned {receipts_count} receipts"
            except Exception as e:
                supports_batch = False
                notes = str(e)
            
            status = "✓ Supported" if supports_batch else "✗ Not supported"
            print(f"{i+1:<3} {rpc_url:<50} {status:<15} {notes[:30]}...")
            
            results.append({
                'rpc_url': rpc_url,
                'supports_batch': supports_batch,
                'notes': notes
            })
            
        except Exception as e:
            print(f"{i+1:<3} {rpc_url:<50} {'! Error':<15} Could not connect: {str(e)[:30]}...")
            results.append({
                'rpc_url': rpc_url,
                'supports_batch': False,
                'notes': f"Connection error: {str(e)}"
            })
    
    print("=" * 80)
    
    supported_count = sum(1 for r in results if r['supports_batch'])
    print(f"\nSUMMARY: {supported_count} out of {len(results)} RPC endpoints support eth_getBlockReceipts")
    
    if supported_count > 0:
        print("\nThe following RPCs support batch receipt fetching:")
        for r in results:
            if r['supports_batch']:
                print(f"- {r['rpc_url']}")
    
    if supported_count < len(results):
        print("\nThe following RPCs do NOT support batch receipt fetching:")
        for r in results:
            if not r['supports_batch']:
                print(f"- {r['rpc_url']}")

if __name__ == "__main__":
    test_batch_receipt_support() 