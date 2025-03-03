import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_stables',
    description='Load Stablecoin balances via RPCs',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,4,21),
    schedule='30 01 * * *'
)

def etl():
    @task()
    def run_stables():
        from src.db_connector import DbConnector
        from src.stables_config import stables_metadata, stables_mapping
        from src.adapters.adapter_stables import AdapterStablecoinSupply

        # Initialize DB Connector
        db_connector = DbConnector()
        days = 3

        # Create adapter params
        adapter_params = {
            'stables_metadata': stables_metadata,
            'stables_mapping': stables_mapping
        }

        # Initialize the Stablecoin Adapter
        stablecoin_adapter = AdapterStablecoinSupply(adapter_params, db_connector)

        # Step 1: Get block data for all chains
        print("Step 1: Collecting block data...")
        block_params = {
            'days': days, 
            'load_type': 'block_data'
        }
        block_df = stablecoin_adapter.extract(block_params)
        stablecoin_adapter.load(block_df)
        print(f"Loaded {len(block_df)} block records")

        # Step 2: Get bridged stablecoin supply
        print("\nStep 2: Collecting bridged stablecoin data...")
        bridged_params = {
            'days': days,
            'load_type': 'bridged_supply'
        }
        bridged_df = stablecoin_adapter.extract(bridged_params)
        stablecoin_adapter.load(bridged_df)
        print(f"Loaded {len(bridged_df)} bridged stablecoin records")

        # Step 3: Get direct stablecoin supply
        print("\nStep 3: Collecting direct stablecoin data...")
        direct_params = {
            'days': days,
            'load_type': 'direct_supply'
        }
        direct_df = stablecoin_adapter.extract(direct_params)
        stablecoin_adapter.load(direct_df)
        print(f"Loaded {len(direct_df)} direct stablecoin records")

        # Step 4: Calculate total stablecoin supply
        print("\nStep 4: Calculating total stablecoin supply...")
        total_params = {
            'days': days,
            'load_type': 'total_supply'
        }
        total_df = stablecoin_adapter.extract(total_params)
        stablecoin_adapter.load(total_df)
        print(f"Loaded {len(total_df)} total supply records")

        print("\nData collection complete!")
    
    run_stables()
etl()