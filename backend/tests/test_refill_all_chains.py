import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from src.db_connector import DbConnector
from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import Web3CC, get_chain_config
from src.main_config import get_main_config

def run_refill_task(chain_name, db_connector, start_date, end_date, batch_size):
    """
    Refills data for a specific chain in the given date range by reprocessing all blocks
    in that range, regardless of whether they exist in the database or not.
    """
    active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
    w3 = None

    for rpc_config in active_rpc_configs:
        try:
            w3 = Web3CC(rpc_config)
            break
        except Exception as e:
            print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")

    if not w3:
        raise ConnectionError("Failed to connect to any provided RPC node.")

    adapter_params = {'chain': chain_name, 'rpc_configs': active_rpc_configs}
    node_adapter = NodeAdapter(adapter_params, db_connector)

    try:
        print(f"Refilling {chain_name} from {start_date} to {end_date}")
        node_adapter.backfill_date_range(start_date, end_date, batch_size)
        print(f"Refill process completed successfully for {chain_name}.")
    except Exception as e:
        print(f"Refiller: An error occurred for {chain_name}: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    # Get the main configuration to determine which chains have backfiller enabled
    main_conf = get_main_config()
    chains_to_refill = [chain.origin_key for chain in main_conf if chain.backfiller_on == True]
    
    # Set date range from April 13, 2024 to today
    start_date = "2024-04-13"
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Starting refill for all chains from {start_date} to {end_date}")
    
    # Process each chain
    for chain_name in chains_to_refill:
        db_connector = DbConnector()
        try:
            # Get the batch size from config
            chain_config = next((chain for chain in main_conf if chain.origin_key == chain_name), None)
            batch_size = chain_config.backfiller_batch_size if chain_config else 10
            
            print(f"\n{'-'*80}\nProcessing {chain_name} with batch size {batch_size}\n{'-'*80}")
            run_refill_task(chain_name, db_connector, start_date, end_date, batch_size)
        except Exception as e:
            print(f"Error processing {chain_name}: {e}")
        finally:
            db_connector.close()
    
    print("\nAll refill tasks completed.") 