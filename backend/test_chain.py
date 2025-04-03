from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config
from src.db_connector import DbConnector

def run_celo():
    # Initialize DbConnector
    db_connector = DbConnector()
    
    chain_name = 'celo'

    active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
    print(f"CELO_CONFIG={active_rpc_configs}")

    adapter_params = {
        'rpc': 'local_node',
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }

    # Initialize NodeAdapter
    adapter = NodeAdapter(adapter_params, db_connector)

    # Initial load parameters
    load_params = {
        'block_start': 31056500,
        'batch_size': batch_size,
    }

    try:
        adapter.extract_raw(load_params)
    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    finally:
        adapter.log_stats()


if __name__ == "__main__":
    run_celo()