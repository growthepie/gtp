import os
from src.adapters.adapter_nader import BaseNodeAdapter
from src.db_connector import DbConnector
adapter_params = {
    'rpc': 'local_node',
    'chain': 'base',
    'node_url': os.getenv("BASE_NODE"),
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
    'block_start': 3384849,
    'batch_size': 10,
}
adapter.extract_raw(load_params)
