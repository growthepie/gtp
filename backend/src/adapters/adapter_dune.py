import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter

from src.queries.dune_queries import dune_queries
from dune_client.client import DuneClient
from dune_client.types import QueryParameter

from src.misc.helper_functions import upsert_to_kpis, get_df_kpis
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterDune(AbstractAdapter):
    """
    adapter_params require the following fields:
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Dune", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']

        self.client = DuneClient(self.api_key)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        query_names:list - the queries that should be loaded. If None, all available queries will be loaded
        days:int - the number of days to load. If auto, the number of days will be determined by the adapter
        load_type:str - can be 'metrics' or 'inscriptions'
    """
    def extract(self, load_params:dict):
        ## Set variables
        query_names = load_params['query_names']
        days = load_params['days']
        self.load_type = load_params['load_type']

        if self.load_type == 'metrics':
            ## Prepare queries to load
            if query_names is not None:
                self.queries_to_load = [x for x in dune_queries if x.name in query_names and x.name != 'inscriptions']
            else:
                self.queries_to_load = dune_queries

            ## Load data
            df = self.extract_data(self.queries_to_load, days)     
            
            print_extract(self.name, load_params,df.shape)
            return df
        elif self.load_type == 'inscriptions':
            self.queries_to_load = [x for x in dune_queries if x.name == 'inscriptions']
            df = self.extract_inscriptions(self.queries_to_load, days)
            print_extract(self.name, load_params,df.shape)
            return df
        else:
            raise NotImplementedError(f"load_type {self.load_type} not implemented")

    def load(self, df:pd.DataFrame):
        if self.load_type == 'metrics':
            upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
            print_load(self.name, upserted, tbl_name)
        elif self.load_type == 'inscriptions':
            tbl_name = 'inscription_addresses'
            upserted = self.db_connector.upsert_table(tbl_name, df)
            print_load(self.name, upserted, tbl_name)
        else:
            raise NotImplementedError(f"load_type {self.load_type} not implemented")

    ## ----------------- Helper functions --------------------
    def prepare_df(self, df):
        ## unpivot df
        df = df.melt(id_vars=['day', 'origin_key'], var_name='metric_key', value_name='value')

        df['date'] = df['day'].apply(pd.to_datetime)
        df['date'] = df['date'].dt.date
        df.drop(['day'], axis=1, inplace=True)
        df['value'] = df['value'].replace('<nil>', 0)
        df.value.fillna(0, inplace=True)
        df['value'] = df['value'].astype(float)
        
        return df

    def extract_data(self, queries_to_load, days):
        dfMain = get_df_kpis()

        for query in queries_to_load:
            if days == 'auto':
                if query.name in ['waa', 'maa']:
                    day_val = 30
                else:
                    day_val = 7
            else:
                day_val = days

            query.params = [QueryParameter.text_type(name="Days", value=str(day_val))]

            print(f"...start loading {query.name} with query_id: {query.query_id} and params: {query.params}")
            df = self.client.refresh_into_dataframe(query)

            df = self.prepare_df(df)
            print(f"...finished loading {query.name}. Loaded {df.shape[0]} rows")
            dfMain = pd.concat([dfMain,df])
            time.sleep(1)

        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def extract_inscriptions(self, query, days):
        if days == 'auto':
            day_val = 1000
        else:
            day_val = days
        
        query[0].params = [QueryParameter.text_type(name="Days", value=str(day_val))]

        print(f"...start loading {query[0].name} with query_id: {query[0].query_id} and params: {query[0].params}")
        df = self.client.refresh_into_dataframe(query[0])

        ##df.address to bytea
        df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
        
        print(f"...finished loading {query[0].name}. Loaded {df.shape[0]} rows")
        df.set_index(['address', 'origin_key'], inplace=True)
        return df