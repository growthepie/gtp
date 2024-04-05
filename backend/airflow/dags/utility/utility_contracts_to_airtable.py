from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.chain_config import adapter_mapping
import src.misc.airtable_functions as at
from eth_utils import to_checksum_address

#initialize Airtable instance
import os
from pyairtable import Api
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=15),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_contracts_to_airtable',
    description='Update Airtable for contract labelling',
    tags=['utility', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='00 02 * * *'
)

def etl():
    @task()
    def read_airtable():
        # read current airtable
        df = at.read_all_labeled_contracts_airtable(table)
        if df is None:
            print("Nothing to upload")
        else:
            # initialize db connection
            db_connector = DbConnector()
            if df[df['sub_category_key'] == 'inscriptions'].empty == False:
                # add to incription table
                df_inscriptions = df[df['sub_category_key'] == 'inscriptions'][['address', 'origin_key']]
                df_inscriptions.set_index(['address', 'origin_key'], inplace=True)
                db_connector.upsert_table('inscription_addresses', df_inscriptions)
            # add to blockspace labels
            df['added_on_time'] = datetime.now()
            df.set_index(['address', 'origin_key'], inplace=True)
            db_connector.upsert_table('blockspace_labels' ,df[df['sub_category_key'] != 'inscriptions'])
            print(f"Uploaded {len(df)} contracts to the database")

    @task()
    def write_airtable():
        # delete every row in airtable
        at.clear_all_airtable(table)
        # db connection
        db_connector = DbConnector()
        # get top unlabelled contracts
        df = db_connector.get_unlabelled_contracts('20', '30')
        df['address'] = df['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
        # add block explorer urls
        block_explorer_mapping = [x for x in adapter_mapping if x.block_explorer is not None]
        for i, row in df.iterrows():
            for m in block_explorer_mapping:
                if row['origin_key'] == m.origin_key:
                    df.at[i, 'Blockexplorer'] = m.block_explorer + '/address/' + row['address']
                    break
        # write to airtable
        at.push_to_airtable(table, df)

    task1 = read_airtable()
    task2 = write_airtable()

    # Set task dependencies
    task1 >> task2
etl()
