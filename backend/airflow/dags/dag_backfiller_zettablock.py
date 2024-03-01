from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.adapters.adapter_gtp_backfill_task import check_and_record_missing_block_ranges
from src.adapters.adapter_raw_zettablock import AdapterZettaBlockRaw


default_args = {
    'owner' : 'mseidl',
    'retries' : 2,
    'email' : ['matthias@orbal-analytics.com'],
    'email_on_failure': True,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = 'dag_backfiller_zettablock_v01',
    description = 'Backfill potentially missing Zettablock data.',
    start_date = datetime(2024,1,29),
    schedule = '10 01 * * *'
)

def etl():
    @task()
    def backfiller_zksync_era():
        tbl = 'zksync_era_tx'
        db_connector = DbConnector()

        adapter_params = {
            'api_key' : os.getenv("ZETTABLOCK_API_2")
        }

        ad = AdapterZettaBlockRaw(adapter_params, db_connector)

        # Calculate the date range for the backfill
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') # 7 days ago

        # Calculate the block range for the backfill
        start_block = db_connector.get_max_block(tbl, start_date) ##from our db
        end_block = ad.get_last_block(tbl) ##from zettablock

        missing_block_ranges = check_and_record_missing_block_ranges(db_connector, tbl, start_block, end_block)
        ## if missing_blocks_ranges is type boolean and False, then no missing blocks were found
        if not missing_block_ranges:
            print(f"No missing blocks found for table: {tbl}.")
        else:
            for i, block_range in enumerate(missing_block_ranges):
                if block_range[1] - block_range[0] > 1: ## more than 1 empty block
                    print(f'{i}/{len(missing_block_ranges)}: extracting block range {block_range}')
                    load_params = {
                        'keys' : [tbl],
                        'block_start' : block_range[0], ## block number as int
                        'block_end' : block_range[1], ## block number as int
                        'step_overwrite' : True
                    }

                    # extract & load incremmentally
                    ad.extract_raw(load_params)

    @task()
    def backfiller_polygon_zkevm():
        tbl = 'polygon_zkevm_tx'
        db_connector = DbConnector()

        # Calculate the date range for the backfill
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') # 7 days ago
        end_date = (datetime.now()).strftime('%Y-%m-%d') # Today

        # Calculate the block range for the backfill
        start_block = db_connector.get_max_block(tbl, start_date)
        end_block = db_connector.get_min_block(tbl, end_date)

        missing_block_ranges = check_and_record_missing_block_ranges(db_connector, tbl, start_block, end_block)

        ## if missing_blocks_ranges is type boolean and False, then no missing blocks were found
        if not missing_block_ranges:
            print(f"No missing blocks found for table: {tbl}.")
        else:
            adapter_params = {
                'api_key' : os.getenv("ZETTABLOCK_API_2")
            }

            ad = AdapterZettaBlockRaw(adapter_params, db_connector)

            for i, block_range in enumerate(missing_block_ranges):
                if block_range[1] - block_range[0] > 1: ## more than 1 empty block
                    print(f'{i}/{len(missing_block_ranges)}: extracting block range {block_range}')
                    load_params = {
                        'keys' : [tbl],
                        'block_start' : block_range[0], ## block number as int
                        'block_end' : block_range[1], ## block number as int
                        'step_overwrite' : True
                    }

                    # extract & load incremmentally
                    ad.extract_raw(load_params)

    backfiller_zksync_era()
    # backfiller_polygon_zkevm()

etl()

