import os
from src.adapters.starknet_adapter import StarkNetAdapter
from src.db_connector import DbConnector

adapter_params = {
    'chain': 'starknet',
    'rpc_url': os.getenv("STARKNET_RPC"),
}

# Initialize DbConnector
db_connector = DbConnector()

# Initialize StarkNetAdapter
adapter = StarkNetAdapter(adapter_params, db_connector)

# Test run method
load_params = {
    'block_start': 'auto',
    'batch_size': 20,
    'threads': 30,
}

try:
    adapter.extract_raw(load_params)
except Exception as e:
    print(f"Extraction stopped due to an exception: {e}")
    raise e
