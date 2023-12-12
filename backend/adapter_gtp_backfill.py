import os
from dotenv import load_dotenv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from sqlalchemy import text
from src.adapters.adapter_utils import *
from src.db_connector import DbConnector

# Constants
CHAIN = "mantle"
BATCH_SIZE = 150
THREADS = 7
TABLE_NAME=CHAIN + "_tx"
MISSING_BLOCKS_FILE= "missing_blocks_" + TABLE_NAME + ".json"

# Load environment variables
load_dotenv()
RPC_URL = os.getenv(CHAIN.upper() + "_RPC")

BUCKET_NAME=os.getenv("S3_LONG_TERM_BUCKET")
DB_NAME = os.getenv("DB_DATABASE")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ------------------ Batch Processing Functions ------------------
def check_and_record_missing_block_ranges(db_connector, table_name, latest_block):

    print(f"Checking and recording missing block ranges for table: {table_name}")

    # Find the smallest block number in the database
    smallest_block_query = text(f"SELECT MIN(block_number) AS min_block FROM {table_name};")
    with db_connector.engine.connect() as connection:
        result = connection.execute(smallest_block_query).fetchone()
        min_block = result[0] if result[0] is not None else 0  # Access the first item in the tuple

    print(f"Starting check from block number: {min_block}, up to latest block: {latest_block}")

    # Adjusted query to start from the smallest block number and go up to the latest block
    query = text(f"""
        WITH RECURSIVE missing_blocks (block_number) AS (
            SELECT {min_block} AS block_number
            UNION ALL
            SELECT block_number + 1
            FROM missing_blocks
            WHERE block_number < {latest_block}
        )
        SELECT mb.block_number
        FROM missing_blocks mb
        LEFT JOIN {table_name} t ON mb.block_number = t.block_number
        WHERE t.block_number IS NULL
        ORDER BY mb.block_number;
    """)

    with db_connector.engine.connect() as connection:
        missing_blocks_result = connection.execute(query).fetchall()

    if not missing_blocks_result:
        print(f"No missing block ranges found for table: {table_name}.")
        return

    missing_ranges = []
    start_missing_range = None
    previous_block = None

    for row in missing_blocks_result:
        current_block = row[0]
        if start_missing_range is None:
            start_missing_range = current_block

        if previous_block is not None and current_block != previous_block + 1:
            missing_ranges.append((start_missing_range, previous_block))
            start_missing_range = current_block

        previous_block = current_block

    # Add the last range if it exists
    if start_missing_range is not None:
        missing_ranges.append((start_missing_range, previous_block))

    # Save to JSON file
    with open(MISSING_BLOCKS_FILE, 'w') as file:
        json.dump(missing_ranges, file)

    print(f"Missing block ranges saved to {MISSING_BLOCKS_FILE}")

def process_missing_blocks_in_batches(db_connector, s3_connection, json_file, batch_size, w3):
    with open(json_file, 'r') as file:
        missing_block_ranges = json.load(file)

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []

        for block_range in missing_block_ranges:
            start_block = block_range[0]
            end_block = block_range[1]

            for batch_start in range(start_block, end_block + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_block)

                future = executor.submit(fetch_and_process_range, batch_start, batch_end, CHAIN, w3, TABLE_NAME, s3_connection, BUCKET_NAME, db_connector)
                futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")

    # After processing all ranges, delete the JSON file
    try:
        os.remove(json_file)
        print(f"Successfully deleted the file: {json_file}")
    except OSError as e:
        print(f"Error: {e.filename} - {e.strerror}.")

def main():    
    # Initialize DbConnector
    db_connector = DbConnector()   
    
    s3_connection, _ = connect_to_s3()
    # Connect to node
    w3 = connect_to_node(RPC_URL)
    if w3 is None:
        print("Failed to connect to the Ethereum node.")
        sys.exit(1)

    # Determine the latest block number
    latest_block = w3.eth.block_number
    print(f"Latest block number: {latest_block}")
    
    # Check and record missing block ranges
    check_and_record_missing_block_ranges(db_connector, TABLE_NAME, latest_block)
    # Process missing blocks in batches 
    process_missing_blocks_in_batches(db_connector, s3_connection, MISSING_BLOCKS_FILE, BATCH_SIZE, w3)
    
if __name__ == "__main__":
    main()