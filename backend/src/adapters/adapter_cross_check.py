import time
import pandas as pd
import os
import io
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.chain_config import adapter_mapping
from src.misc.helper_functions import return_projects_to_load, upsert_to_kpis, check_projects_to_load, api_get_call
from src.misc.helper_functions import print_init, print_load, send_discord_message

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
        self.proxy =  {
            'https': os.getenv('PROXY'),
        }
        self.webhook_url = os.getenv('DISCORD_ALERTS')
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
        metric_key = 'txcount_explorer'

        for project in projects_to_load:
            print(f"... loading {project.origin_key} txcount data from explorer ({project.block_explorer_type})...")
            
            try:
                if project.block_explorer_type == 'etherscan':
                    response = api_get_call(project.block_explorer_txcount, header = self.headers, as_json=False, proxy=self.proxy)
                    
                    data = io.StringIO(response)
                    df = pd.read_csv(data)

                    df['date'] = pd.to_datetime(df['Date(UTC)'])
                    df['metric_key'] = metric_key
                    df['origin_key'] = project.origin_key
                    df.rename(columns={'Value': 'value'}, inplace=True)
                    df = df[['date', 'metric_key', 'origin_key', 'value']]

                    dfMain = pd.concat([dfMain, df], ignore_index=True)

                elif project.block_explorer_type == 'blockscout':
                    response = api_get_call(project.block_explorer_txcount, header = self.headers, proxy=self.proxy)
                    df = pd.DataFrame(response['chart_data'])

                    df['date'] = pd.to_datetime(df['date'])
                    df['metric_key'] = metric_key
                    df['origin_key'] = project.origin_key
                    df.rename(columns={'tx_count': 'value'}, inplace=True)
                    df = df[['date', 'metric_key', 'origin_key', 'value']]

                    dfMain = pd.concat([dfMain, df], ignore_index=True)        

                elif project.block_explorer_type == 'l2beat':
                    response_json = api_get_call(project.block_explorer_txcount, sleeper=10, retries=20)
                    df = pd.json_normalize(response_json['daily'], record_path=['data'], sep='_')

                    if project.origin_key == 'ethereum':
                        df = df.iloc[:,[0,2]]
                        df.rename(columns={2:'value'}, inplace=True)
                    else:
                        ## only keep the columns 0 (date) and 1 (transactions)
                        df = df.iloc[:,[0,1]]                     
                        df.rename(columns={1:'value'}, inplace=True)

                    df['date'] = pd.to_datetime(df[0],unit='s').dt.date
                    df.drop([0], axis=1, inplace=True)
                    df['metric_key'] = metric_key
                    df['origin_key'] = project.origin_key

                    dfMain = pd.concat([dfMain, df], ignore_index=True)  

                elif project.block_explorer_type == 'NA':
                    print(f"no block explorer defined for {project.origin_key} - moving on...")
                
                else:
                    print(f'not implemented {project.block_explorer_type}')
                    raise ValueError('Block Explorer Type not supported')
            except Exception as e:
                print(f"TxCount Cross Check: Error loading comparison txcount data for {project.origin_key}: {e}")
                send_discord_message(f"Error loading txcount data for {project.origin_key}: {e}", self.webhook_url)
                continue
        
        today = datetime.today().strftime('%Y-%m-%d')
        dfMain.drop(dfMain[dfMain.date == today].index, inplace=True, errors='ignore')
        dfMain.value.fillna(0, inplace=True)

        dfMain.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return dfMain

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)     

    def cross_check(self):
        exec_string = """
            with temp as (
            SELECT 
                origin_key,
                SUM(case when metric_key = 'txcount_raw' then value end) as raw,
                SUM(case when metric_key = 'txcount_explorer' then value end) as explorer
            FROM fact_kpis 
            WHERE metric_key in ('txcount_raw', 'txcount_explorer')
            and date < date_trunc('day', NOW()) 
            and date >= date_trunc('day',now()) - interval '7 days' 
            group by 1
        )

        select 
            *, 
            explorer - raw as diff,
            (explorer - raw) / explorer as diff_percent
            from temp
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        for index, row in df.iterrows():
            if row['origin_key'] == 'rhino':
                threshold = 0.6
            else:
                threshold = 0.05

            if row['diff_percent'] > threshold:
                send_discord_message(f"txcount discrepancy in last 7 days for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({int(row['diff'])} tx)", self.webhook_url)
                print(f"txcount discrepancy for {row['origin_key']}: {row['diff_percent'] * 100:.2f}% ({int(row['diff'])})")