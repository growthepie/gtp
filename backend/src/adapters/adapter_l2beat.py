import time
import pandas as pd
from datetime import datetime
from lxml import html

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterL2Beat(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("L2Beat", adapter_params, db_connector)
        self.base_url = 'https://l2beat.com/api/'
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        main_conf = get_main_config(self.db_connector)

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

            naming = chain.l2beat_tvl_naming
            url = f"{self.base_url}tvl/{naming}.json"           

            response_json = api_get_call(url, sleeper=10, retries=20)
            df = pd.json_normalize(response_json['daily'], record_path=['data'], sep='_')

            ## only keep the columns 0 (date) and 1 (total tvl)
            df = df.iloc[:,[0,1]]

            df['date'] = pd.to_datetime(df[0],unit='s')
            df['date'] = df['date'].dt.date
            df.drop(df[df[1] == 0].index, inplace=True)
            df.drop([0], axis=1, inplace=True)
            df.rename(columns={1:'value'}, inplace=True)
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

        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def extract_stages(self, projects_to_load):
        stages = []
        for chain in projects_to_load:
            origin_key = chain.origin_key
            print(f'...loading stage info for {origin_key}')
            url = f"https://l2beat.com/scaling/projects/{chain.l2beat_tvl_naming}"
            response = api_get_call(url, as_json=False)
            tree = html.fromstring(response)
            #element = tree.xpath('/html/body/div[4]/header/div[1]/div[3]/div[2]/li[4]/span/span/a/div/span/span')
            element = tree.xpath('/html/body/div[1]/div[4]/div/main/div[2]/header/div[1]/div[3]/div[2]/li[4]/span/span/a/div/span/span')
            if len(element) == 0:
                stage = 'NA'
            else:
                stage = element[0].xpath('string()')

            
            stages.append({'origin_key': origin_key, 'l2beat_stage': stage})
            print(f"...{self.name} - loaded Stage: {stage} for {origin_key}")
            time.sleep(0.5)
        df = pd.DataFrame(stages)
        return df