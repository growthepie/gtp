import sys
from datetime import datetime, timedelta
from src.db_connector import DbConnector
from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import Web3CC, get_chain_config
from src.adapters.rpc_funcs.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day, find_last_block_of_day

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
        node_adapter.backfill_missing_blocks(start_block, end_block, batch_size)
        print("Backfill process completed successfully.")
    except Exception as e:
        print(f"Backfiller: An error occurred: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    batch_size = 10
    db_connector = DbConnector()
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    chain_names = [
        "arbitrum_nova",
        "zircuit",
        "metis",
        "mantle",
        "fraxtal",
        "ink",
        "mint",
        "real",
        "soneium",
        "swell",
        "unichain",
        "polygon_zkevm",
        "celo",
    ]
    for chain_name in chain_names:
        print(f"Starting backfill for {chain_name} from {start_date} to {end_date}")
        try:
            run_backfill_task(chain_name, db_connector, start_date, end_date, batch_size)
        except Exception as e:
            print(f"Backfiller: An error occurred while processing {chain_name}: {e}", file=sys.stderr)

    print("All backfill tasks completed.")