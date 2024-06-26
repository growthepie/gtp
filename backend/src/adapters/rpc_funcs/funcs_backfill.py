import os
from dotenv import load_dotenv
from datetime import datetime
import time
from sqlalchemy import text


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