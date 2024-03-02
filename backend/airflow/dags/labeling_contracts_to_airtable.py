from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task
from src.db_connector import DbConnector
import src.misc.airtable_functions as at
from eth_utils import to_checksum_address

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email' : ['lorenz@growthepie.xyz', 'manish@growthepie.xyz', 'matthias@growthepie.xyz'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=15)
    },
    dag_id='labeling_contracts_to_airtable',
    description='Update Airtable for contract labelling',
    tags=['labeling', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='00 02 * * *'
)

def etl():
    @task()
    def read_airtable():
        # read current airtable
        df = at.read_all_airtable()
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

    @task()
    def write_airtable():
        # delete every row in airtable
        at.clear_all_airtable()
        # db connection
        db_connector = DbConnector()
        # get top unlabelled contracts
        df = db_connector.get_unlabelled_contracts('20', '30')
        df['address'] = df['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
        # write to airtable
        at.push_to_airtable(df)

    task1 = read_airtable()
    task2 = write_airtable()

    # Set task dependencies
    task1 >> task2
etl()

