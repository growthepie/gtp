import os
from src.adapters.loopring_adapter import LoopringAdapter
from src.db_connector import DbConnector

adapter_params = {
    'chain': 'loopring',
    'rpc_urls': [os.getenv("RPC_URL_1"), os.getenv("RPC_URL_2")],
}


# Initialize DbConnector
db_connector = DbConnector()

# Initialize LoopringAdapter
adapter = LoopringAdapter(adapter_params, db_connector)

# Test run method
load_params = {
    'block_start': 'auto',
    'batch_size': 1,
    'threads': 1,
}

try:
    adapter.extract_raw(load_params)
except Exception as e:
    print(f"Extraction stopped due to an exception: {e}")
    raise e

