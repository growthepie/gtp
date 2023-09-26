import os
from src.adapters.adapter_nader_super import BaseNodeAdapter
from src.db_connector import DbConnector
adapter_params = {
    'rpc': 'local_node',
    'chain': 'zora',
    'node_url': os.getenv("ZORA_RPC"),
}

# Initialize DbConnector
db_connector = DbConnector()

# Initialize BaseNodeAdapter
adapter = BaseNodeAdapter(adapter_params, db_connector)

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
    'batch_size': 1,
    'threads': 1,
}
adapter.extract_raw(load_params)