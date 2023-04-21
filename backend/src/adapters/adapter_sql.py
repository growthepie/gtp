import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.queries.sql_queries import sql_queries
from src.misc.helper_functions import upsert_to_kpis, get_missing_days_kpis
from src.misc.helper_functions import print_init, print_load, print_extract, check_projects_to_load

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterSQL(AbstractAdapter):
    """
    adapter_params require the following fields
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("SQL Aggregation", adapter_params, db_connector)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        load_type:str - can be 'usd_to_eth' or 'metrics'
        days:str - days of historical data that should be loaded, starting from today.
        origin_keys:list - list of origin_keys
        metric_keys:list - the metrics that should be loaded. If None, all available metrics will be loaded
    """
    def extract(self, load_params:dict):
        ## Set variables
        load_type = load_params['load_type']
        days = load_params['days']

        ## aggregation types
        if load_type == 'usd_to_eth': ## also make sure to add new metrics in db_connector
            raw_metrics = ['tvl', 'rent_paid_usd', 'fees_paid_usd', 'stables_mcap']
            df = self.db_connector.get_values_in_eth(raw_metrics, days)
        elif load_type == 'metrics':
            origin_keys = load_params['origin_keys']
            metric_keys = load_params['metric_keys']
            days = load_params['days']

            ## Prepare queries to load
            check_projects_to_load(sql_queries, origin_keys)
            if origin_keys is not None:
                self.queries_to_load = [x for x in sql_queries if x.origin_key in origin_keys]
            else:
                self.queries_to_load = sql_queries
            if metric_keys is not None:
                self.queries_to_load = [x for x in self.queries_to_load if x.metric_key in metric_keys]
            else:
                self.queries_to_load = self.queries_to_load

            ## Load data
            df = self.extract_data_from_db(self.queries_to_load, days)
        else:
            raise ValueError('load_type not supported')

        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        df.value.fillna(0, inplace=True)

        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    def extract_data_from_db(self, queries_to_load, days):
        dfMain = pd.DataFrame()
        for query in queries_to_load:
            if days == 'auto':
                if query.origin_key == 'multi':
                    day_val = 40 ### that should be improved....
                else:
                    day_val = get_missing_days_kpis(self.db_connector, metric_key= query.metric_key, origin_key=query.origin_key)
            else:
                day_val = days
            query.update_query_parameters({'Days': day_val})
            
            print(f"... executing query: {query.metric_key} - {query.origin_key} with {query.query_parameters} days")
            df = pd.read_sql(query.sql, self.db_connector.engine.connect())
            df['date'] = df['day'].apply(pd.to_datetime)
            df['date'] = df['date'].dt.date
            df.drop(['day'], axis=1, inplace=True)
            df.rename(columns= {'val':'value'}, inplace=True)
            df.rename(columns= {'value':'value'}, inplace=True)
            df['metric_key'] = query.metric_key
            if 'origin_key' not in df.columns:
                df['origin_key'] = query.origin_key
            df.value.fillna(0, inplace=True)

            dfMain = pd.concat([dfMain, df], ignore_index=True)
            print(f"...query loaded: {query.metric_key} {query.origin_key} with {day_val} days. DF shape: {df.shape}")
        return dfMain
