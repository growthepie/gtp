import pandas as pd
import json
import os

from src.adapters.clients.bigquery import BigQuery
from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import api_post_call, send_discord_message, print_init, print_load, print_extract

#TODO: add website, twitter, github org once available

class AdapterOSO(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OSO", adapter_params, db_connector)
        google_creds = adapter_params['google_creds']
        self.bq = BigQuery(google_creds)
        self.webhook_url = adapter_params['webhook']
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        
    """
    def extract(self, load_params:dict):
        df = self.extract_oss()

        print_extract(self.name, load_params, df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        tbl_name = 'oli_oss_directory'     
        self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, df.shape, tbl_name)

    ## ----------------- Helper functions --------------------

    def load_oss_projects(self):
        ## Load the data from BigQuery
        query = """
                SELECT 
                project_name,
                display_name,
                project_namespace,
                project_source,
                description
            FROM `growthepie.oso_production.projects_v1`
        """

        df = self.bq.execute_bigquery(query)
        df = df.rename(columns={'project_name': 'name', 'project_namespace': 'namespace', 'project_source': 'source'})
        return df

    def load_oss_github_slugs(self):
        ## Load the data from BigQuery
        query = """
                WITH org_repos AS (
                    SELECT 
                        project_name,
                        artifact_namespace AS github_org,
                        COUNT(*) AS count_repos,
                        ROW_NUMBER() OVER (PARTITION BY project_name ORDER BY COUNT(*) DESC) AS row_num
                    FROM `growthepie.oso_production.artifacts_by_project_v1`
                    WHERE artifact_source = 'GITHUB'
                    GROUP BY project_name, artifact_namespace
                )
                SELECT 
                    project_name,
                    github_org as github_slug
                FROM org_repos
                WHERE row_num = 1;
        """

        df = self.bq.execute_bigquery(query)
        ## rename column project_slug to name and github_slug to main_github
        df = df.rename(columns={'project_name': 'name', 'github_slug': 'main_github'})
        return df

    ## This method loads the projects and github slugs data and joins them on project_id
    ## It returns a dataframe with the following columns: ['project_id', 'project_name', 'project_slug', 'user_namespace', 'github_org']
    def get_oss_projects(self):
        df_projects = self.load_oss_projects()
        # df_github_slugs = self.load_oss_github_slugs()


        ## TODO: reverse this change once the github slugs are available
        # ## join the two dataframes on project_id
        # df = pd.merge(df_projects, df_github_slugs, on='name', how='left')
        # df['active'] = True

        # return df
        df_projects['active'] = True
        return df_projects	

    ## Projects that are in our db (df_active_projects) but not in the export from OSS (df_oss) are dropped projects
    ## These projects will get deactivated in our DB and we send a notifcation in our Discord about it
    def deactivate_dropped_projects(self, df_oss, df_active_projects):
        if df_oss.shape[0] > 1800:
            df_dropped_projects = df_active_projects[~df_active_projects['name'].isin(df_oss['name'])].copy()
            dropped_projects = df_dropped_projects['name'].to_list()
            print(f"...{len(dropped_projects)} projects were dropped since the last sync: {dropped_projects}")

            if len(dropped_projects) > 0:
                self.db_connector.deactivate_projects(dropped_projects)

                send_discord_message(f"OSS projects DROPPED. We might need to update our tag_mapping table for label 'owner_project': {dropped_projects}", self.webhook_url)
            else:
                print("Nothing to deactivate")
        else:
            raise Exception("The number of projects in the OSS export is too low. Something went wrong.")
        
    ## Combine all above functions to get the final df and deactivate dropped projects
    def extract_oss(self):
        ## get latest oss projects from their endpoints and get our active projects in our db
        df_oss = self.get_oss_projects()
        df_active_projects = self.db_connector.get_active_projects()

        ## deactivate projects in our db that don't appear anymore in the oss endpoints
        self.deactivate_dropped_projects(df_oss, df_active_projects)

        ## identify new projects send a notification in Discord
        df_new_projects = df_oss[~df_oss['name'].isin(df_active_projects['name'])]
        new_projects = df_new_projects['name'].to_list()
        print(f"...{len(new_projects)} projects newly added since the last sync: {new_projects}")
        if len(new_projects) > 0:
            send_discord_message(f"<@874921624720257037> OSS projects newly ADDED: {new_projects}", self.webhook_url)

        # drop the namespace column
        df_oss.drop(columns=['namespace'], inplace=True)

        ## set index
        df_oss.set_index('name', inplace=True)

        return df_oss
    
    def check_inactive_projects(self):
        print("Checking for inactive projects with contracts assigned")
        df = self.db_connector.get_tags_inactive_projects()
        if df.shape[0] > 0:
            print(f"Projects that currently inactive but have contracts assigned (need to be updated in oli_tag_mapping): {df['value'].to_list()}")
            send_discord_message(f"<@874921624720257037> Projects that currently inactive but have contracts assigned (need to be updated in oli_tag_mapping): {df['value'].to_list()}", self.webhook_url)
        else:
            print("No inactive projects with contracts assigned")


