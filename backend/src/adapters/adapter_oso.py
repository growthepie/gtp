import pandas as pd
from github import Github # pip install PyGithub
import zipfile
import io
import yaml
import requests

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import api_post_call, send_discord_message, print_init, print_load, print_extract

class AdapterOSO(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OSO", adapter_params, db_connector)
        self.g = Github(adapter_params['github_token'])
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

    ## This method loads the projects from oss-directory github
    ## It returns a df with columns: ['name', 'display_name', 'description', 'github', 'websites', 'npm', 'social', 'active', 'source']
    def load_oss_projects(self):
        
        # Get the repository
        repo_url = "https://github.com/opensource-observer/oss-directory/tree/main/data/projects"
        _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')
        path = '/'.join(path)
        repo = self.g.get_repo(f"{owner}/{repo_name}")

        # Download oss-directory as ZIP file
        zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
        response = requests.get(zip_url)
        zip_content = io.BytesIO(response.content)

        # Convert ZIP to df of projects
        df = pd.DataFrame()
        with zipfile.ZipFile(zip_content) as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.yaml') and file_name.startswith(f"{repo_name}-{branch}/{path}"):
                    with zip_ref.open(file_name) as file:
                        content = file.read().decode('utf-8')
                        content = yaml.safe_load(content)
                        df_temp = pd.json_normalize(content)
                        if 'social' in content:
                            df_temp['social'] = [content['social']]
                        df = pd.concat([df, df_temp], ignore_index=True)

        # prepare df_projects for inserting into our db table 'oli_oss_directory'
        df = df[['name', 'display_name', 'description', 'github', 'websites', 'npm', 'social']]
        df['active'] = True # project is marked active because it is in the OSS directory
        df['source'] = 'OSS_DIRECTORY'

        # to be removed columns!!!
        df['main_github'] = df['github'].apply(lambda x: x[0]['url'] if isinstance(x, list) else None) # get the first github url
        df['main_github'] = df['main_github'].apply(lambda x: x.split('/')[-1] if isinstance(x, str) else None) # remove the url and keep only the name of the github repo
        df['website'] = df['websites'].apply(lambda x: x[0]['url'] if isinstance(x, list) else None) # get the first website url
        df['twitter'] = df['social'].apply(lambda x: x['twitter'][0]['url'] if not pd.isna(x) and 'twitter' in x and isinstance(x['twitter'], list) else None) # get the first X url
         
        return df	

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
        
    ## Combine the above functions to get the final df and deactivate dropped projects
    def extract_oss(self):
        ## get latest oss projects from oss-directory Github repo and our active projects in our db
        df_oss = self.load_oss_projects()
        df_active_projects = self.db_connector.get_active_projects()

        ## deactivate projects in our db that don't appear anymore in the oss-directory 
        self.deactivate_dropped_projects(df_oss, df_active_projects)

        ## identify new projects send a notification in Discord
        df_new_projects = df_oss[~df_oss['name'].isin(df_active_projects['name'])]
        new_projects = df_new_projects['name'].to_list()
        print(f"...{len(new_projects)} projects newly added since the last sync: {new_projects}")
        if len(new_projects) > 0:
            send_discord_message(f"<@874921624720257037> OSS projects newly ADDED: {new_projects}", self.webhook_url)

        ## set index
        df_oss.set_index('name', inplace=True)

        return df_oss
    
    # not used as of now, current work in progress
    def check_inactive_projects(self):
        print("Checking for inactive projects with contracts assigned")
        df = self.db_connector.get_tags_inactive_projects()
        if df.shape[0] > 0:
            print(f"Projects that currently inactive but have contracts assigned (need to be updated in oli_tag_mapping): {df['value'].to_list()}")
            send_discord_message(f"<@874921624720257037> Projects that currently inactive but have contracts assigned (need to be updated in oli_tag_mapping): {df['value'].to_list()}", self.webhook_url)
        else:
            print("No inactive projects with contracts assigned")


