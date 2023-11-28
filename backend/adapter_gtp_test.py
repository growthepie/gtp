import os
from src.adapters.adapter_raw_gtp import NodeAdapter, MaxWaitTimeExceededException
from src.db_connector import DbConnector

adapter_params = {
    'rpc': 'local_node',
    'chain': 'linea',
    'node_url': os.getenv("LINEA_RPC"),
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
    'block_start': 786098, # block 5th Nov 
    'batch_size': 200,
    'threads': 4,
}

try:
    adapter.extract_raw(load_params)
except MaxWaitTimeExceededException as e:
    print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
    raise e