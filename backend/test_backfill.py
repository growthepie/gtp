import sys
from src.adapters.rpc_funcs.utils import get_chain_config
from src.adapters.adapter_raw_rpc import NodeAdapter
from src.db_connector import DbConnector

def run_backfill_date_range(chain_name, db_connector, start_date, end_date, batch_size):
    """
    Standalone function to backfill blockchain data for a specific date range.

    Args:
        chain_name (str): The name of the blockchain chain.
        db_connector: The database connector instance.
        start_date (str): The start date for backfill in YYYY-MM-DD format.
        end_date (str): The end date for backfill in YYYY-MM-DD format.
        batch_size (int): The batch size for processing.
    """
    # Fetch active RPC configurations
    active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)

    # Initialize NodeAdapter with parameters
    adapter_params = {'chain': chain_name, 'rpc_configs': active_rpc_configs}
    node_adapter = NodeAdapter(adapter_params, db_connector)

    try:
        # Run backfill for the specified date range
        node_adapter.backfill_date_range(start_date, end_date, batch_size)
        print("Backfill process completed successfully.")
    except Exception as e:
        print(f"Backfiller: An error occurred: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    # Initialize database connector
    db_connector = DbConnector()

    # Configuration
    chain_name = "fraxtal"
    start_date = "2024-08-17"  # Start date for backfill
    end_date = "2024-08-24"    # End date for backfill
    batch_size = 10            # Number of blocks to process per batch

    # Run the backfill task
    run_backfill_date_range(chain_name, db_connector, start_date, end_date, batch_size)
