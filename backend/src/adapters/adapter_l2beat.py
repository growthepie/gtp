import time
import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from backend.src.chain_config import adapter_mapping
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
        ## Set variables
        origin_keys = load_params['origin_keys']

        projects = [x for x in adapter_mapping if x.l2beat_tvl_naming is not None]
        
        ## Prepare projects to load (can be a subset of all projects)
        check_projects_to_load(projects, origin_keys)
        projects_to_load = return_projects_to_load(projects, origin_keys)

        ## Load data
        df = self.extract_data(
            projects_to_load=projects_to_load
            ,base_url=self.base_url
            )

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    def extract_data(self, projects_to_load, base_url):
        dfMain = get_df_kpis()
        for adapter_mapping in projects_to_load:
            origin_key = adapter_mapping.origin_key

            naming = adapter_mapping.l2beat_tvl_naming
            url = f"{base_url}tvl/{naming}.json"           

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
            max_date = df['date'].max()
            df.drop(df[df.date == max_date].index, inplace=True)
            today = datetime.today().strftime('%Y-%m-%d')
            df.drop(df[df.date == today].index, inplace=True, errors='ignore')
            df.value.fillna(0, inplace=True)
            dfMain = pd.concat([dfMain,df])

            print(f"...{self.name} - loaded for {origin_key}. Shape: {df.shape}")
            time.sleep(1)

        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain