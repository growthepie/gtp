import time
import pandas as pd
from datetime import datetime
from lxml import html
import os

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract, send_discord_message


class AdapterL2Beat(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("L2Beat", adapter_params, db_connector)
        self.base_url = 'https://l2beat.com/api/'
        self.webhook = os.getenv('DISCORD_ALERTS')
        self.webhook_analyst = os.getenv('GTP_AI_WEBHOOK_URL')
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        main_conf = get_main_config()

        ## Set variables
        origin_keys = load_params['origin_keys']
        self.load_type = load_params['load_type']

        projects = [chain for chain in main_conf if chain.aliases_l2beat is not None]
        
        ## Prepare projects to load (can be a subset of all projects)
        check_projects_to_load(projects, origin_keys)
        projects_to_load = return_projects_to_load(projects, origin_keys)

        ## Load data
        if self.load_type == 'tvl':
            df = self.extract_tvl(
                projects_to_load=projects_to_load)
        elif self.load_type == 'stages':
            df = self.extract_stages(
                projects_to_load=projects_to_load)
        else:
            raise NotImplementedError(f"load_type {self.load_type} not recognized")

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        if self.load_type == 'tvl':
            upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
            print_load(self.name, upserted, tbl_name)
        elif self.load_type == 'stages':
            self.db_connector.update_sys_chains(df, 'str')
            print_load(self.name, df.shape, 'sys_chains')

    ## ----------------- Helper functions --------------------

    def extract_tvl(self, projects_to_load):
        dfMain = get_df_kpis()
        for chain in projects_to_load:
            origin_key = chain.origin_key
            
            if origin_key == 'ethereum':
                continue

            naming = chain.aliases_l2beat_slug
            url = f"https://l2beat.com/api/scaling/tvl/{naming}?range=max"       
            print(url)
            response_json = api_get_call(url, sleeper=10, retries=3)
            if response_json['success']:
                df = pd.json_normalize(response_json['data']['chart'], record_path=['data'], sep='_')

                ## only keep the columns 0 (date), 1 (canonical tvl), 2 (external tvl), 3 (native tvl)
                df = df.iloc[:,[0,1,2,3]]
                df['date'] = pd.to_datetime(df[0],unit='s')
                df['date'] = df['date'].dt.date

                df.drop([0], axis=1, inplace=True)
                ## sum column 1,2,3
                df['value'] = df.iloc[:,0:3].sum(axis=1)
                ## drop the 3 tvl columns
                df = df[['date','value']]
                df['metric_key'] = 'tvl'
                df['origin_key'] = origin_key
                # max_date = df['date'].max()
                # df.drop(df[df.date == max_date].index, inplace=True)
                today = datetime.today().strftime('%Y-%m-%d')
                df.drop(df[df.date == today].index, inplace=True, errors='ignore')
                df.value.fillna(0, inplace=True)
                dfMain = pd.concat([dfMain,df])

                print(f"...{self.name} - loaded TVL for {origin_key}. Shape: {df.shape}")
                time.sleep(1)
            else:
                print(f'Error loading TVL data for {origin_key}')
                send_discord_message(f'L2Beat: Error loading TVL data for {origin_key}. Other chains are not impacted.', self.webhook)            

        dfMain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def extract_stages(self, projects_to_load):
        stages = []
        url = f"https://l2beat.com/api/scaling/summary"
        response = api_get_call(url)

        # Get current main_config to compare against new L2Beat values
        current_config = get_main_config()

        for chain in projects_to_load:
            origin_key = chain.origin_key
            if origin_key == 'ethereum':
                continue
            
            l2beat_id = str(chain.aliases_l2beat)
            print(f'...loading stage info for {origin_key} with l2beat_id: {l2beat_id}') 
            stage = response['data']['projects'][l2beat_id]['stage']

            if stage in ['NotApplicable', 'Not applicable']:
                stage = 'NA'
                
            # Compare with the existing stage in the main_config
            current_stage = next((config.l2beat_stage for config in current_config if config.origin_key == origin_key), 'NA')

            # If the stage has changed, send a notification
            if stage != current_stage:
                send_discord_message(f'L2Beat: {origin_key} has changed stage from {current_stage} to {stage}', self.webhook)
            
            stages.append({'origin_key': origin_key, 'l2beat_stage': stage})
            print(f"...{self.name} - loaded Stage: {stage} for {origin_key}")
            time.sleep(0.5)
            
        df = pd.DataFrame(stages)
        return df