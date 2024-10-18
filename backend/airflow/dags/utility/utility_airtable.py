from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.main_config import get_main_config
import src.misc.airtable_functions as at
from eth_utils import to_checksum_address

#initialize Airtable instance
import os
import pandas as pd
import numpy as np
from pyairtable import Api
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
api = Api(AIRTABLE_API_KEY)

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='utility_airtable',
    description='Update Airtable for contracts labelling',
    tags=['utility', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='15 02 * * *'
)

def etl():
    @task()
    def read_airtable_contracts():
        # read current airtable
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')
        df = at.read_all_labeled_contracts_airtable(api, AIRTABLE_BASE_ID, table)
        if df is None:
            print("Nothing to upload")
        else:
            # initialize db connection
            db_connector = DbConnector()

            if df[df['usage_category'] == 'inscriptions'].empty == False:
                # add to incription table
                df_inscriptions = df[df['usage_category'] == 'inscriptions'][['address', 'origin_key']]
                df_inscriptions.set_index(['address', 'origin_key'], inplace=True)
                db_connector.upsert_table('inscription_addresses', df_inscriptions)

            # add to oli_tag_mapping table
            df = df[df['usage_category'] != 'inscriptions']

            ## remove duplicates address, origin_key
            df.drop_duplicates(subset=['address', 'origin_key'], inplace=True)

            ## keep columns address, origin_key, labelling type and unpivot the other columns
            df = df.melt(id_vars=['address', 'origin_key', 'source'], var_name='tag_id', value_name='value')
            ## filter out rows with empty values
            df = df[df['value'].notnull()]
            df['added_on'] = datetime.now()
            df.set_index(['address', 'origin_key', 'tag_id'], inplace=True)

            db_connector.upsert_table('oli_tag_mapping', df)
            print(f"Uploaded {len(df)} labels to the database")

    @task()
    def run_refresh_materialized_view(read_airtable_contracts:str):
        # refresh the materialized view for OLI tags, so not the same contracts are shown in the airtable
        db_connector = DbConnector()
        db_connector.refresh_materialized_view('vw_oli_labels_materialized')

    @task()
    def oss_projects(run_refresh_materialized_view:str):
        # db connection and airtable connection
        db_connector = DbConnector()
        table = api.table(AIRTABLE_BASE_ID, 'OSS Projects')

        # delete every row in airtable
        at.clear_all_airtable(table)
        # get active projects from db
        df = db_connector.get_projects_for_airtable()
        # write to airtable
        at.push_to_airtable(table, df)

    @task()
    def write_airtable_contracts(oss_projects:str):
        # db connection and airtable connection
        db_connector = DbConnector()
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')
        main_config = get_main_config(db_connector)

        at.clear_all_airtable(table)
        
        # get top unlabelled contracts, short and long term and also inactive contracts
        df0 = db_connector.get_unlabelled_contracts('16', '1000') # top 16 contracts per chain from last 1000 days
        df1 = db_connector.get_unlabelled_contracts('16', '365') # top 16 contracts per chain from last year
        df2 = db_connector.get_unlabelled_contracts('16', '7') # top 16 contracts per chain from last week
        df3 = db_connector.get_inactive_contracts() # inactive contracts
        df3['internal_description'] = 'project ' + df3['old_owner_project'] + ' needs to be remapped'
        df3 = df3.drop(columns=['old_owner_project'])
        
        # merge the all dataframes & reset index
        df = pd.concat([df0, df1, df2, df3])
        df = df.reset_index(drop=True)

        # remove duplicates
        df = df.drop_duplicates(subset=['address', 'origin_key'])

        # checksum the addresses
        df['address'] = df['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))

        # add block explorer urls
        block_explorer_mapping = [chain for chain in main_config if chain.block_explorers is not None]
        for i, row in df.iterrows():
            for m in block_explorer_mapping:
                if row['origin_key'] == m.origin_key:
                    df.at[i, 'Blockexplorer'] = next(iter(m.block_explorers.values())) + '/address/' + row['address']
                    break
        df = df[df['Blockexplorer'].notnull()]

        # exchange the category with the id & make it a list
        cat = api.table(AIRTABLE_BASE_ID, 'Usage Categories')
        df_cat = at.read_airtable(cat)
        df = df.replace({'usage_category': df_cat.set_index('Category')['id']})
        df['usage_category'] = df['usage_category'].apply(lambda x: [x])

        # exchange the project with the id & make it a list
        proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
        df_proj = at.read_airtable(proj)
        df = df.replace({'owner_project': df_proj.set_index('Name')['id']})
        df['owner_project'] = df['owner_project'].apply(lambda x: [x])

        # write to airtable
        at.push_to_airtable(table, df)

    # order the tasks
    write_airtable_contracts(oss_projects(run_refresh_materialized_view(read_airtable_contracts())))

etl()
