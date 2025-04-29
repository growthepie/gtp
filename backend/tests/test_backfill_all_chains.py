import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from src.db_connector import DbConnector
from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import Web3CC, get_chain_config
from src.adapters.rpc_funcs.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day, find_last_block_of_day
from src.main_config import get_main_config

def run_backfill_task(chain_name, db_connector, start_date, end_date, batch_size):
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

    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_timestamp = date_to_unix_timestamp(start_date_obj.year, start_date_obj.month, start_date_obj.day)
    end_timestamp = date_to_unix_timestamp(end_date_obj.year, end_date_obj.month, end_date_obj.day)
    start_block = find_first_block_of_day(w3, start_timestamp)
    end_block = find_last_block_of_day(w3, end_timestamp)

    adapter_params = {'chain': chain_name, 'rpc_configs': active_rpc_configs}
    node_adapter = NodeAdapter(adapter_params, db_connector)

    try:
        print(f"Backfilling {chain_name} from block {start_block} to {end_block}")
        node_adapter.backfill_missing_blocks(start_block, end_block, batch_size)
        print(f"Backfill process completed successfully for {chain_name}.")
    except Exception as e:
        print(f"Backfiller: An error occurred for {chain_name}: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    # Get the main configuration to determine which chains have backfiller enabled
    main_conf = get_main_config()
    chains_to_backfill = [chain.origin_key for chain in main_conf if chain.backfiller_on == True]
    
    # Yesterday's date (April 14, 2024)
    start_date = "2024-04-14"
    end_date = "2024-04-14"  # Same day for a single day backfill
    
    print(f"Starting backfill for all chains from {start_date} to {end_date}")
    
    # Process each chain
    for chain_name in chains_to_backfill:
        db_connector = DbConnector()
        try:
            # Get the batch size from config
            chain_config = next((chain for chain in main_conf if chain.origin_key == chain_name), None)
            batch_size = chain_config.backfiller_batch_size if chain_config else 10
            
            print(f"\n{'-'*80}\nProcessing {chain_name} with batch size {batch_size}\n{'-'*80}")
            run_backfill_task(chain_name, db_connector, start_date, end_date, batch_size)
        except Exception as e:
            print(f"Error processing {chain_name}: {e}")
        finally:
            db_connector.close()
    
    print("\nAll backfill tasks completed.") 