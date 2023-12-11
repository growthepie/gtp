import os
from src.adapters.adapter_raw_gtp import NodeAdapter
from src.db_connector import DbConnector
from src.adapters.adapter_utils import *

adapter_params = {
    'rpc': 'local_node',
    'chain': 'mantle',
    'node_url': os.getenv("MANTLE_RPC"),
}

# Initialize DbConnector
db_connector = DbConnector()

# Initialize NodeAdapter
adapter = NodeAdapter(adapter_params, db_connector)

# Test run method
load_params = {
    'block_start': 'auto',
    'batch_size': 150,
    'threads': 7,
}

try:
    adapter.extract_raw(load_params)
except MaxWaitTimeExceededException as e:
    print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
    raise e