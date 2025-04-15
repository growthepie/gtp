from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
from src.db_connector import DbConnector

def run_ethereum():
    """
    Test script for Ethereum data loading.
    Starts from block 22265969 and uses batch receipt functionality.
    """
    # Initialize DbConnector
    db_connector = DbConnector()
    
    chain_name = 'ethereum'

    active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
    print(f"ETH_CONFIG={active_rpc_configs}")

    # Adjust workers to 1 for testing to avoid overwhelming RPCs
    for config in active_rpc_configs:
        config['workers'] = 1

    adapter_params = {
        'rpc': 'local_node',
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }

    # Initialize NodeAdapter
    adapter = NodeAdapter(adapter_params, db_connector)

    # Initial load parameters
    load_params = {
        'block_start': 22265969,
        'batch_size': 10,
    }

    try:
        adapter.extract_raw(load_params)
    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    finally:
        adapter.log_stats()


if __name__ == "__main__":
    run_ethereum() 