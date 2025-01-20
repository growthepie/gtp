import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

class AdapterDune(AbstractAdapter):
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Dune", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']
        self.client = DuneClient(self.api_key)
        print_init(self.name, self.adapter_params)

    def extract(self, load_params:dict):
        """
        Extract utilizes the `load_params` dictionary to execute specified queries, prepare the resulting data and load it into a desired table.

        Parameters:
        
        load_params : dict
            - 'queries' (list): A list of dictionaries, each specifying a query to be executed. Each query dictionary contains:
                - 'name' (str): A descriptive name for the query.
                - 'query_id' (int): The unique identifier for the query.
                - 'params' (dict): Parameters for the query, can be any as set in dune:
                    - 'days' (int): The time range for the query in days.
                    - 'chain' (str): The origin_key of the chain.
            - 'prepare_df' (str): The name of the function or method to be used for preparing the resulting df into the desired format.
            - 'load_type' (str): Specifies the table to load the df into.
        """
        self.load_params = load_params

        # create list of QueryBase objects
        self.queries = []
        for query in self.load_params.get('queries'):
            self.queries.append(QueryBase(name = query['name'], query_id = query['query_id']))
            if 'params' in query:
                self.queries[-1].params = [QueryParameter.text_type(name = k, value = v) for k, v in query['params'].items()]

        # load all queries and merge them into one dataframe
        df_main = pd.DataFrame()
        for query in self.queries:
            try:
                print(f"...start loading {query.name} with query_id: {query.query_id} and params: {query.params}")
                df = self.client.refresh_into_dataframe(query)
                print(f"...finished loading {query.name}. Loaded {df.shape[0]} rows")
            except Exception as e:
                print(f"Error loading {query.name}: {e}")
                continue
            
            # Prepare df if set in load_params
            prep_df = self.load_params.get('prepare_df')
            if prep_df != None:
                df = eval(f"self.{prep_df}(df)")
            
            # Concatenate dataframes into one
            df_main = pd.concat([df_main, df])

        print_extract(self.name, self.load_params, df_main.shape)
        return df_main

    def load(self, df:pd.DataFrame):
        table = self.load_params.get('load_type')
        if table != None:
            try:
                upserted = self.db_connector.upsert_table(table, df)
                print_load(self.name, upserted, table)
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print("No load_type specified in load_params. Data not loaded!")
        
        
    ## ----------------- Helper functions --------------------
    
    def prepare_df_metric_daily(self, df):
        # unpivot df only if not already in the correct format
        if 'metric_key' not in df.columns and 'value' not in df.columns:
            df = df.melt(id_vars=['day', 'origin_key'], var_name='metric_key', value_name='value')
        # change day column to date
        df['date'] = df['day'].apply(pd.to_datetime).dt.date
        df.drop(['day'], axis=1, inplace=True)
        # replace nil or None values with 0
        df['value'] = df['value'].replace('<nil>', 0)
        df = df.value.fillna(0)
        # turn value column into float
        df['value'] = df['value'].astype(float)
        # set primary keys as index
        df = df.set_index(['metric_key', 'origin_key', 'date'])
        return df
    
    def prepare_df_incriptions(self, df):
        # address column to bytea
        df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
        # set primary keys as index
        df = df.set_index(['address', 'origin_key'])
        return df
    
    def prepare_df_glo_holders(self, df):
        # address column to bytea
        df['address'] = df['address'].apply(lambda x: bytes.fromhex(x[2:]))
        # date column with current date
        df['date'] = datetime.now().date()
        # parse origin_keys column in df so that it can be loaded into a postgres array - split by comma and add curly braces
        df['origin_keys'] = df['origin_keys'].apply(lambda x: '{"' + x.replace(',', '","') + '"}')
        # set primary keys as index
        df = df.set_index(['address', 'date'])
        return df
