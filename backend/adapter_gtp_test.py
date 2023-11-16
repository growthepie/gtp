import os
from src.adapters.adapter_raw_gtp import NodeAdapter
from src.db_connector import DbConnector

# List of RPC URLs
RPC_URLS = [
    "https://1rpc.io/celo",
    "https://rpc.ankr.com/celo",
    "https://forno.celo.org",
    "https://celo.api.onfinality.io/public",
]

# Current index for the RPC URLs
current_rpc_index = 0

def get_next_rpc_url():
    global current_rpc_index
    rpc_url = RPC_URLS[current_rpc_index]
    current_rpc_index = (current_rpc_index + 1) % len(RPC_URLS)
    return rpc_url

# Using the get_next_rpc_url function to set the initial RPC for the adapter
adapter_params = {
    'rpc': 'local_node',
    'chain': 'celo',
    'node_url': get_next_rpc_url(),
}

# Initialize DbConnector
db_connector = DbConnector()

# Initialize NodeAdapter
adapter = NodeAdapter(adapter_params, db_connector)

# Test database connectivity
if not adapter.check_db_connection():
    print("Failed to connect to database.")
else:
    print("Successfully connected to database.")

# Test S3 connectivity
if not adapter.check_s3_connection():
    print("Failed to connect to S3.")
else:
    print("Successfully connected to S3.")

# Test run method
load_params = {
    'block_start': 'auto',
    'batch_size': 10,
    'threads': 5,
}

#adapter.extract_raw(load_params)

def extract_with_rotation(adapter, load_params):
    batch_count = load_params.get('batch_size', 1)

    for _ in range(batch_count): 
        new_rpc_url = get_next_rpc_url()         # Get the next RPC URL
        adapter.set_rpc_url(new_rpc_url)         # Update the RPC URL in the adapter
        adapter.extract_raw(load_params)         # Call the extract_raw method

extract_with_rotation(adapter, load_params)