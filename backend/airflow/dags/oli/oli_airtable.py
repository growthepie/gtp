import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook


@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_airtable',
    description='Update Airtable for contracts labelling',
    tags=['oli', 'daily'],
    start_date=datetime(2023,9,10),
    schedule='20 01 * * *' #after coingecko and after sql_blockspace, before sql materialize
)

def etl():
    @task()
    def airtable_read_attest_contracts():
        import os
        import time
        import pandas as pd
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from src.misc.oli import OLI

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        # initialize db connection
        db_connector = DbConnector()
        # initialize OLI instance
        oli = OLI(os.getenv("OLI_gtp_pk"), is_production=True)

        # read current airtable labels
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')
        df = at.read_all_labeled_contracts_airtable(api, AIRTABLE_BASE_ID, table)
        if df is None:
            print("No new labels detected")
        else:
            # remove duplicates address, chain_id
            df.drop_duplicates(subset=['address', 'chain_id'], inplace=True)
            # keep columns address, chain_id, labelling type and unpivot the other columns
            df = df.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
            # filter out rows with empty values & make address lower
            df = df[df['value'].notnull()]
            df['address'] = df['address'].str.lower() # important, as db addresses are all lower

        # go one by one and attest the new labels
        for group in df.groupby(['address', 'chain_id']):
            # add source column to keep track of origin
            df_air = group[1]
            df_air['source'] = 'airtable'
            # get label from gold table to fill in missing tags
            df_gold = db_connector.get_oli_trusted_label_gold(group[0][0], group[0][1])
            df_gold = df_gold[['address', 'caip2', 'tag_id', 'value']]
            df_gold = df_gold.rename(columns={'caip2': 'chain_id'})
            df_gold['source'] = 'gold_table'
            # merge (if not empty) and drop duplicates, keep airtable over gold tags in case of duplicates
            if df_gold.empty:
                df_merged = df_air
            else:
                df_merged = pd.concat([df_air, df_gold], ignore_index=True)
            df_merged = df_merged.drop_duplicates(subset=['address', 'chain_id', 'tag_id'], keep='first')
            df_merged = df_merged.drop(columns=['source'])
            # attest the label
            tags = df_merged.set_index('tag_id')['value'].to_dict() 
            address = group[0][0]
            chain_id = group[0][1]
            # call the API
            max_attempts = 5 # API has many internal server errors, therefor we retry x times
            for attempt in range(1, max_attempts + 1):
                # function to create the offchain label
                response = oli.create_offchain_label(address, chain_id, tags)
                if response.status_code == 200:
                    break
                else:
                    print(f"Attempt {attempt} failed: {response.text}")
                    time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print("Failed to get a 200 response after 5 attempts.")
                print(f"Address: {address}")
                print(f"Chain ID: {chain_id}")
                print(f"Tags: {tags}")
                raise ValueError(f"Final error: {response.text}")

    @task()
    def refresh_trusted_entities(): # TODO: add new tags automatically to public.oli_tags from OLI github
        from src.misc.helper_functions import get_trusted_entities
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # get trusted entities from gtp-dna Github, rows with '*' are expanded based on public.oli_tags
        df = get_trusted_entities(db_connector)

        # turn attester into bytea
        df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])

        # set attester & tag_id as index
        df = df.set_index(['attester', 'tag_id'])

        # upsert to oli_trusted_entities table, making sure to delete first for full refresh
        db_connector.delete_all_rows('oli_trusted_entities')
        db_connector.upsert_table('oli_trusted_entities', df)
    
    @task()
    def run_refresh_materialized_view():
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # refresh the materialized view for OLI tags, so not the same contracts are shown in the airtable
        db_connector.refresh_materialized_view('vw_oli_labels_materialized') # TODO: remove all dependencies and move to vw_oli_label_pool_gold

        # also refresh the materialized view for the oli_label_pool_gold
        db_connector.refresh_materialized_view('vw_oli_label_pool_gold')

    @task()
    def write_oss_projects():
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

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
    def write_chain_info():
        import pandas as pd
        import os
        import src.misc.airtable_functions as at
        from pyairtable import Api
        from src.main_config import get_main_config

        # initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # get all chain info from main config
        config = get_main_config()
        df = pd.DataFrame(data={'origin_key': [chain.origin_key for chain in config],
                                'chain_type': [chain.chain_type for chain in config],
                                'l2beat_stage': [chain.l2beat_stage for chain in config],
                                'caip2': [chain.caip2 for chain in config],
                                'name': [chain.name for chain in config],
                                'name_short': [chain.name_short for chain in config],
                                'bucket': [chain.bucket for chain in config],
                                'block_explorers': [None if chain.block_explorers is None else next(iter(chain.block_explorers.values())) for chain in config],
                                'socials_website': [chain.socials_website for chain in config],
                                'socials_twitter': [chain.socials_twitter for chain in config],
                                'runs_aggregate_blockspace': [chain.runs_aggregate_blockspace for chain in config]
                                })

        # merge current table with the new data
        table = api.table(AIRTABLE_BASE_ID, 'Chain List')
        df = df.merge(at.read_airtable(table)[['origin_key', 'id']], how='left', left_on='origin_key', right_on='origin_key')

        # update existing records (primary key is the id)
        at.update_airtable(table, df)

        # add any new records/chains if present
        df_new = df[df['id'].isnull()]
        if df_new.empty == False:
            at.push_to_airtable(table, df_new.drop(columns=['id']))

    @task()
    def write_airtable_contracts():
        import os
        import pandas as pd
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from eth_utils import to_checksum_address

        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # db connection and airtable connection
        db_connector = DbConnector()
        table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')

        at.clear_all_airtable(table)
        
        # get top unlabelled contracts, short and long term and also inactive contracts
        df0 = db_connector.get_unlabelled_contracts('20', '720') # top 20 contracts per chain from last 720 days
        df1 = db_connector.get_unlabelled_contracts('20', '180') # top 20 contracts per chain from last 3 months
        df2 = db_connector.get_unlabelled_contracts('20', '30') # top 20 contracts per chain from last month
        df3 = db_connector.get_unlabelled_contracts('20', '7') # top 20 contracts per chain from last week
        
        # merge the all dataframes & reset index
        df = pd.concat([df0, df1, df2, df3])
        df = df.reset_index(drop=True)

        # remove duplicates
        df = df.drop_duplicates(subset=['address', 'origin_key'])

        # checksum the addresses
        df['address'] = df['address'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))

        # remove all duplicates that are still in the airtable due to temp_owner_project
        df_remove = at.read_airtable(table)
        if df_remove.empty == False:
            # replace id with actual origin_key
            chains = api.table(AIRTABLE_BASE_ID, 'Chain List')
            df_chains = at.read_airtable(chains)
            df_remove['origin_key'] = df_remove['origin_key'].apply(lambda x: x[0])
            df_remove = df_remove.replace({'origin_key': df_chains.set_index('id')['origin_key']})
            # remove duplicates from df
            df_remove = df_remove[['address', 'origin_key']]
            df = df.merge(df_remove, on=['address', 'origin_key'], how='left', indicator=True)
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

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

        # exchange the chain origin_key with the id & make it a list
        chains = api.table(AIRTABLE_BASE_ID, 'Chain List')
        df_chains = at.read_airtable(chains)
        df = df.replace({'origin_key': df_chains.set_index('origin_key')['id']})
        df['origin_key'] = df['origin_key'].apply(lambda x: [x])

        # write to airtable
        at.push_to_airtable(table, df)

    @task()
    def write_depreciated_owner_project():
        import os
        from pyairtable import Api
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from src.misc.helper_functions import send_discord_message
        
        #initialize Airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)

        # clear the whole table
        table = api.table(AIRTABLE_BASE_ID, 'Remap Owner Project')
        at.clear_all_airtable(table)

        # db connection
        db_connector = DbConnector()

        # get inactive projects
        df = db_connector.get_tags_inactive_projects()

        # send alert to discord
        if df.shape[0] > 0:
            print(f"Inactive contracts found: {df['value'].unique().tolist()}")
            send_discord_message(f"<@874921624720257037> Inactive projects with assigned contracts (update in oli_tag_mapping): {df['value'].unique().tolist()}", os.getenv('DISCORD_CONTRACTS'))
        else:
            print("No inactive projects with contracts assigned found")

        # group by owner_project and then write to airtable
        df = df.rename(columns={'value': 'old_owner_project'})
        df = df.groupby('old_owner_project').size()
        df = df.reset_index(name='count')
        at.push_to_airtable(table, df)

    @task()
    def airtable_read_label_pool_reattest():
        # read in approved labels & delete approved labels from airtable
        from src.db_connector import DbConnector
        import src.misc.airtable_functions as at
        from pyairtable import Api
        from src.misc.oli import OLI
        import pandas as pd
        import time
        import os
        # initialize db connection
        db_connector = DbConnector()
        # airtable instance
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')
        # OLI instance
        oli = OLI(os.getenv("OLI_gtp_pk"), is_production=False)
        # read all approved labels in 'Label Pool Reattest'
        df = at.read_all_approved_label_pool_reattest(api, AIRTABLE_BASE_ID, table)
        if df is None:
            print("No labels upserted.")
        else:
            # remove duplicates address, origin_key
            df = df.drop_duplicates(subset=['address', 'chain_id'])
            # keep track of ids
            ids = df['id'].tolist()
            # capitalize the first letter for contract_name
            df['contract_name'] = df['contract_name'].str.capitalize()
            # keep columns address, origin_key and unpivot the other columns
            df = df[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
            df = df.melt(id_vars=['address', 'chain_id'], var_name='tag_id', value_name='value')
            # turn address into lower case (IMPORTANT for later concatenation)
            df['address'] = df['address'].str.lower()
            # filter out rows with empty values
            df = df[df['value'].notnull()]
            # go one by one and attest the new labels
            for group in df.groupby(['address', 'chain_id']):
                # add source column to keep track of origin
                df_air = group[1]
                df_air['source'] = 'airtable'
                # get label from gold table to fill in missing tags
                df_gold = db_connector.get_oli_trusted_label_gold(group[0][0], group[0][1])
                df_gold = df_gold[['address', 'caip2', 'tag_id', 'value']]
                df_gold = df_gold.rename(columns={'caip2': 'chain_id'})
                df_gold['source'] = 'gold_table'
                # merge (if not empty) and drop duplicates, keep airtable over gold tags in case of duplicates
                if df_gold.empty:
                    df_merged = df_air
                else:
                    df_merged = pd.concat([df_air, df_gold], ignore_index=True)
                df_merged = df_merged.drop_duplicates(subset=['address', 'chain_id', 'tag_id'], keep='first')
                df_merged = df_merged.drop(columns=['source'])
                # attest the label
                tags = df_merged.set_index('tag_id')['value'].to_dict()
                address = group[0][0]
                chain_id = group[0][1]
                # call the API
                max_attempts = 5 # API has many internal server errors, therefor we retry x times
                for attempt in range(1, max_attempts + 1):
                    # function to create the offchain label
                    response = oli.create_offchain_label(address, chain_id, tags)
                    if response.status_code == 200:
                        break
                    else:
                        print(f"Attempt {attempt} failed: {response.text}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print("Failed to get a 200 response after 5 attempts.")
                    print(f"Address: {address}")
                    print(f"Chain ID: {chain_id}")
                    print(f"Tags: {tags}")
                    raise ValueError(f"Final error: {response.text}")
            # at the end delete just uploaded rows from airtable
            at.delete_airtable_ids(table, ids)

    @task()
    def airtable_read_depreciated_owner_project(self):
        pass
        # read in table 'Remap Owner Project', then reattest all new names

    # all tasks
    read_contracts = airtable_read_attest_contracts()
    read_pool = airtable_read_label_pool_reattest()
    # read_remap = airtable_read_depreciated_owner_project()
    trusted_entities = refresh_trusted_entities()
    refresh = run_refresh_materialized_view()
    write_oss = write_oss_projects()
    write_chain = write_chain_info()
    write_contracts = write_airtable_contracts()
    write_owner_project = write_depreciated_owner_project()

    # Define execution order
    read_contracts >> read_pool >> trusted_entities >> refresh >> write_oss >> write_chain >> write_contracts >> write_owner_project

etl()