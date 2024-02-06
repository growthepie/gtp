import time
import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.chain_config import adapter_mapping
from src.adapters.clients.defillama import DefiLlama
from src.misc.helper_functions import return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterDefiLlama(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("DefiLlama", adapter_params, db_connector)
        self.llama = DefiLlama()
        self.projects = [x for x in adapter_mapping if x.defillama_stablecoin is not None]
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        ## Set variables
        origin_keys = load_params['origin_keys']
        
        ## Prepare projects to load (can be a subset of all projects)
        check_projects_to_load(self.projects, origin_keys)
        projects_to_load = return_projects_to_load(self.projects, origin_keys)

        ## Load data
        df = self.extract_data(
            projects_to_load=projects_to_load
            )

        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)


    ## ----------------- Helper functions --------------------

    def extract_data(self, projects_to_load):
        dfMain = get_df_kpis()
        for adapter_mapping in projects_to_load:
            origin_key = adapter_mapping.origin_key
          
            naming = adapter_mapping.defillama_stablecoin
            origin_key = adapter_mapping.origin_key

            df_all = self.llama.get_stablecoin_hist_mcap('')
            df_all.rename(columns={'totalCirculating':'total'}, inplace=True)
            time.sleep(1)

            df_chain = self.llama.get_stablecoin_hist_mcap_on_a_chain('', naming)
            df_chain.rename(columns={'totalCirculating':'chain'}, inplace=True)
            df_joined = pd.concat([df_chain, df_all], axis=1)
            df_joined['dominance'] = df_joined['chain'] / df_joined['total']
            df_joined.reset_index(inplace=True)

            df = df_joined[['date','chain']].copy()
            df = self.prepare_df(df, origin_key, 'stables_mcap')
            dfMain = pd.concat([dfMain,df])

            df = df_joined[['date','dominance']].copy()
            df = self.prepare_df(df, origin_key, 'stables_dominance')
            dfMain = pd.concat([dfMain,df])

            print(f"...{self.name} - stables_mcap/dominance loaded for {origin_key}. Shape: {df.shape[0] * 2}")
            time.sleep(0.5)

        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain

    def prepare_df(self, df, origin_key, metric_key):
        df.reset_index(inplace=True)
        df['metric_key'] = metric_key
        df['origin_key'] = origin_key
        df['date'] =  pd.to_datetime(df['date']).dt.date
        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        max_date = df['date'].max()
        df.drop(df[df.date == max_date].index, inplace=True)
        today = datetime.today().strftime('%Y-%m-%d')
        df.drop(df[df.date == today].index, inplace=True, errors='ignore')
        df.rename(columns= {'tvl':'value', 'volume':'value', 'dominance':'value', 'chain':'value'}, inplace=True, errors='ignore')
        df.value.fillna(0, inplace=True)
        df = df[['metric_key', 'origin_key', 'date', 'value']]
        return df