import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

import pandas as pd

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='backfill_economics',
    description='Backfill economics and da values for new entries or changes in gtp_dna economics mapping',
    tags=['backfill', 'daily'],
    start_date=datetime(2023, 9, 1),
    schedule_interval='30 09 * * *'
)

def main():
        
    # task to check if a new commit for the file economics_mapping.yml was made in the last 24 hours, returns new or depreciated rows as a df
    @task()
    def check_for_new_commits() -> pd.DataFrame:
        from src.misc.helper_functions import convert_economics_mapping_into_df
        from github import Github
        from datetime import datetime, timedelta, timezone
        import pandas as pd
        import yaml

        d = 1 # check if anything changed in the last d*24 hours 
        repo_name = "growthepie/gtp-dna"
        file_path = "economics_da/economics_mapping.yml"
        branch = "main"
        g = Github()
        repo = g.get_repo(repo_name)

        # get the latest commits
        commits = repo.get_commits(path=file_path, sha=branch)

        # check if there was a new commit in the last d days hours
        if commits[0].commit.author.date < datetime.now(timezone.utc) - timedelta(days=d):
            print(f"No new commit found in the last {24*d} hours.")
            return pd.DataFrame() # return empty df
        else:
            print(f"New commit found in the last {24*d} hours.")

        # get the commit to reconstruct the file how it was d days ago
        commit_24h_ago = None
        for commit in commits:
            if commit.commit.author.date < datetime.now(timezone.utc) - timedelta(days=d):
                commit_24h_ago = commit # how the file looked like 24h ago
                break

        # get the file content at that commit and turn into df
        f_now = repo.get_contents(file_path, ref=branch).decoded_content.decode()
        f_24h_ago = repo.get_contents(file_path, ref=commit_24h_ago.sha).decoded_content.decode()
        df_now = convert_economics_mapping_into_df(yaml.safe_load(f_now))
        df_24h_ago = convert_economics_mapping_into_df(yaml.safe_load(f_24h_ago))
        
        # Find rows that were added
        new_rows = df_now.merge(df_24h_ago, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

        # Find rows that were removed
        depreciated_rows = df_24h_ago.merge(df_now, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

        # merge the two dataframes
        return pd.concat([new_rows, depreciated_rows])


    # task to backfill raw dune data based on df
    def backfill_dune(df: pd.DataFrame):
        # return in case the df is empty
        if df.empty:
            print("No new or depreciated rows in the economics mapping file.")
            return

        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        print("There are new or depreciated rows in the economics mapping file.")

        # group the df
        df = df.groupby(['name', 'origin_key', 'da_layer']).size().reset_index(name='count')
        print(df)

        # initialize adapter
        db_connector = DbConnector()
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        print("Initializing AdapterDune...")
        ad = AdapterDune(adapter_params, db_connector)
        print("AdapterDune initialized.")

        for i, row in df.iterrows():

            ## changes made to the L1 mapping
            if row['da_layer'] == 'l1':
                print(f"Backfilling {row['name']} for Ethereum L1")

                # pulling in raw data from dune:
                load_params = {
                    'queries': [
                        {
                            'name': 'l1-values-per-chain-chain-filter',
                            'query_id': 4561949,
                            'params': {
                                'chain': row['origin_key'],
                            }
                        }
                    ],
                    'prepare_df': 'prepare_df_metric_daily',
                    'load_type': 'fact_kpis'
                }
                df_dune = ad.extract(load_params)
                ad.load(df_dune)

                # ... TODO recalculating metrics in fact_kpis

            ## changes made to the beacon chain mapping
            elif row['da_layer'] == 'beacon':
                print(f"Backfilling {row['name']} for Ethereum Beacon Chain")

                # pulling in raw data from dune:
                load_params = {
                    'queries': [
                        {
                            'name': 'beacon-values-per-chain-chain-filter',
                            'query_id': 4561837,
                            'params': {
                                'chain': row['origin_key'],
                            }
                        }
                    ],
                    'prepare_df': 'prepare_df_metric_daily',
                    'load_type': 'fact_kpis'
                }
                df_dune = ad.extract(load_params)
                ad.load(df_dune)

                # ... TODO recalculating metrics in fact_kpis

            ## changes made to the celestia mapping
            elif row['da_layer'] == 'celestia':
                print(f"Backfilling {row['name']} for Celestia")

                # ... TODO recalculating raw metric in fact_kpis for celestia
                # ... TODO recalculating metrics in fact_kpis

            print(f"Completed backfilling: {row['name']}.")

    # Task dependencies
    df = check_for_new_commits()
    backfill_dune(df)

main()