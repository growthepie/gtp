import os
from dotenv import load_dotenv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
from sqlalchemy import text
from src.adapters.funcs_rps_utils import *
from src.db_connector import DbConnector


# Load environment variables
load_dotenv()

BUCKET_NAME=os.getenv("S3_LONG_TERM_BUCKET")
DB_NAME = os.getenv("DB_DATABASE")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ------------------ Batch Processing Functions ------------------
def check_and_record_missing_block_ranges(db_connector, table_name, start_block, end_block):
    print(f"Checking and recording missing block ranges for table: {table_name}")

    # Ensure start_block is not less than the smallest block in the database
    smallest_block_query = text(f"SELECT MIN(block_number) AS min_block FROM {table_name};")
    with db_connector.engine.connect() as connection:
        result = connection.execute(smallest_block_query).fetchone()
        db_min_block = result[0] if result[0] is not None else 0

    if start_block < db_min_block:
        start_block = db_min_block

    print(f"Starting check from block number: {start_block}, up to block number: {end_block}")

    # Adjusted query to start from the specified start block and go up to the specified end block
    query = text(f"""
        WITH RECURSIVE missing_blocks (block_number) AS (
            SELECT {start_block} AS block_number
            UNION ALL
            SELECT block_number + 1
            FROM missing_blocks
            WHERE block_number < {end_block}
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
        return []

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

    # # Save to JSON file
    # with open(missing_blocks_file, 'w') as file:
    #     json.dump(missing_ranges, file)

    # print(f"Missing block ranges saved to {missing_blocks_file}")
    return missing_ranges

def process_missing_blocks_in_batches(db_connector, s3_connection, missing_block_ranges, batch_size, threads, chain_name, table_name, w3):
    # with open(json_file, 'r') as file:
    #     missing_block_ranges = json.load(file)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []

        for block_range in missing_block_ranges:
            start_block = block_range[0]
            end_block = block_range[1]

            for batch_start in range(start_block, end_block + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_block)

                future = executor.submit(fetch_and_process_range, batch_start, batch_end, chain_name, w3, table_name, s3_connection, BUCKET_NAME, db_connector)
                futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")

    # # After processing all ranges, delete the JSON file
    # try:
    #     os.remove(json_file)
    #     print(f"Successfully deleted the file: {json_file}")
    # except OSError as e:
    #     print(f"Error: {e.filename} - {e.strerror}.")

def date_to_unix_timestamp(year, month, day):
    return int(time.mktime(datetime(year, month, day).timetuple()))

def find_first_block_of_day(w3, target_timestamp):
    min_block = 0
    max_block = w3.eth.block_number
    while min_block <= max_block:
        mid_block = (min_block + max_block) // 2
        mid_block_timestamp = w3.eth.get_block(mid_block).timestamp

        if mid_block_timestamp < target_timestamp:
            min_block = mid_block + 1
        elif mid_block_timestamp > target_timestamp:
            max_block = mid_block - 1
        else:
            # Find the first block of the day
            while w3.eth.get_block(mid_block - 1).timestamp >= target_timestamp:
                mid_block -= 1
            return mid_block

    return min_block

def find_last_block_of_day(w3, target_timestamp):
    # Set the end of the day timestamp (23:59:59 of the target day)
    end_of_day_timestamp = target_timestamp + 86400 - 1  # 86400 seconds in a day, -1 to stay in the same day

    min_block = 0
    max_block = w3.eth.block_number
    while min_block <= max_block:
        mid_block = (min_block + max_block) // 2
        mid_block_timestamp = w3.eth.get_block(mid_block).timestamp

        if mid_block_timestamp < end_of_day_timestamp:
            min_block = mid_block + 1
        elif mid_block_timestamp > end_of_day_timestamp:
            max_block = mid_block - 1
        else:
            # Find the last block of the day
            while mid_block < max_block and w3.eth.get_block(mid_block + 1).timestamp <= end_of_day_timestamp:
                mid_block += 1
            return mid_block

    return max_block  # The last block that is still within the day

def backfiller_task(chain_name, start_date, end_date, threads, batch_size):
    # Initialize DbConnector
    db_connector = DbConnector()   
    
    s3_connection, _ = connect_to_s3()

    # Dynamic RPC URL based on chain name
    rpc_url = os.getenv(chain_name.upper() + "_RPC")

    # Connect to node
    w3 = connect_to_node(rpc_url)
    if w3 is None:
        print("Failed to connect to the Ethereum node.")
        sys.exit(1)

    # Convert start_date and end_date from strings to datetime objects
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    # Convert dates to timestamps
    start_timestamp = date_to_unix_timestamp(start_date_obj.year, start_date_obj.month, start_date_obj.day)
    end_timestamp = date_to_unix_timestamp(end_date_obj.year, end_date_obj.month, end_date_obj.day)

    # Determine the block number range based on dates
    start_block = find_first_block_of_day(w3, start_timestamp)
    end_block = find_last_block_of_day(w3, end_timestamp)

    # Dynamic chain-based table name
    table_name = chain_name + "_tx"

    # # Dynamic file name based on chain name
    # missing_blocks_file = f"missing_blocks_{table_name}.json"

    print(f"Checking blocks from {start_block} to {end_block} in {table_name}")
    
    # Check and record missing block ranges
    missing_blocks_ranges = check_and_record_missing_block_ranges(db_connector, table_name, start_block, end_block)

    ## if missing_blocks_ranges is type boolean and False, then no missing blocks were found
    if not missing_blocks_ranges:
        print(f"No missing blocks found for table: {table_name}.")
    else:
        # Process missing blocks in batches 
        process_missing_blocks_in_batches(db_connector, s3_connection, missing_blocks_ranges, batch_size, threads, chain_name, table_name, w3)
