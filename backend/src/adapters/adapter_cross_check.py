import time
import pandas as pd
import requests
import io
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.adapters.mapping import adapter_mapping
from src.misc.helper_functions import return_projects_to_load, upsert_to_kpis, check_projects_to_load
from src.misc.helper_functions import print_init, print_load

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterCrossCheck(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Cross-Check", adapter_params, db_connector)
        self.projects = [x for x in adapter_mapping if x.block_explorer_txcount is not None]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        origin_keys = load_params['origin_keys']

        check_projects_to_load(self.projects, origin_keys)
        projects_to_load = return_projects_to_load(self.projects, origin_keys)

        dfMain = pd.DataFrame()

        for project in projects_to_load:
            print(f"... loading {project.origin_key} txcount data from explorer ({project.block_explorer_type})...")
            
            if project.block_explorer_type == 'etherscan':
                response = requests.get(project.block_explorer_txcount, headers=self.headers)
                data = io.StringIO(response.text)
                df = pd.read_csv(data)

                print(response.text)
                print(df.columns)

                df['date'] = pd.to_datetime(df['Date(UTC)'])
                df['metric_key'] = 'txcount_explorer'
                df['origin_key'] = project.origin_key
                df.rename(columns={'Value': 'value'}, inplace=True)
                df = df[['date', 'metric_key', 'origin_key', 'value']]
                dfMain = pd.concat([dfMain, df], ignore_index=True)

            elif project.block_explorer_type == 'blockscout':
                response = requests.get('https://zksync2-mainnet.zkscan.io/api/v2/stats/charts/transactions', headers=self.headers)
                df = pd.DataFrame(response.json()['chart_data'])

                df['date'] = pd.to_datetime(df['date'])
                df['metric_key'] = 'txcount_explorer'
                df['origin_key'] = project.origin_key
                df.rename(columns={'tx_count': 'value'}, inplace=True)
                df = df[['date', 'metric_key', 'origin_key', 'value']]
                dfMain = pd.concat([dfMain, df], ignore_index=True)        
            else:
                print(f'not implemented {project.block_explorer_type}')
                raise ValueError('Block Explorer Type not supported')
            
            time.sleep(1)
        
        today = datetime.today().strftime('%Y-%m-%d')
        dfMain.drop(dfMain[dfMain.date == today].index, inplace=True, errors='ignore')
        dfMain.value.fillna(0, inplace=True)

        dfMain.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return dfMain

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)       