import os
from src.adapters.adapter_raw_gtp import NodeAdapter
from src.db_connector import DbConnector
from src.adapters.adapter_utils import *

adapter_params = {
    'rpc': 'local_node',
    'chain': 'arbitrum',
    'rpc_urls': [os.getenv("ARBITRUM_RPC_URL_1"), os.getenv("ARBITRUM_RPC_URL_2")],
    'max_calls_per_rpc': {
        os.getenv("ARBITRUM_RPC_URL_1"): 50,
        os.getenv("ARBITRUM_RPC_URL_2"): 55,
    }
}

# Initialize DbConnector
db_connector = DbConnector()

# Initialize NodeAdapter
adapter = NodeAdapter(adapter_params, db_connector)

# Test run method
load_params = {
    'block_start': 'auto',
    'batch_size': 150,
    'threads': 3,
}

try:
    adapter.extract_raw(load_params)
except MaxWaitTimeExceededException as e:
    print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
    raise e