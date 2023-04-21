import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.queries.zettablock_queries import zettablock_queries
from src.adapters.clients.zettablock_api import ZettaBlock_API
from src.misc.helper_functions import upsert_to_kpis, get_df_kpis, check_projects_to_load, get_missing_days_kpis
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterZettablock(AbstractAdapter):
    """
    adapter_params require the following fields:
        api_key:str - api key for interacting with ZettaBlock API
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ZettaBlock", adapter_params, db_connector)
        self.api_key = adapter_params['api_key']
        self.client = ZettaBlock_API(self.api_key)
        
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
        metric_keys:list - the metrics that should be loaded. If None, all available metrics will be loaded
        days_to_load:int - the number of days to load. If auto, the number of days will be determined by the adapter
    """
    def extract(self, load_params:dict):
        ## Set variables
        origin_keys = load_params['origin_keys']
        metric_keys = load_params['metric_keys']
        days = load_params['days']

        ## Prepare queries to load
        check_projects_to_load(zettablock_queries, origin_keys)        
        if origin_keys is not None:
            self.queries_to_load = [x for x in zettablock_queries if x.origin_key in origin_keys]
        else:
            self.queries_to_load = zettablock_queries
        if metric_keys is not None:
            self.queries_to_load = [x for x in self.queries_to_load if x.metric_key in metric_keys]
        else:
            self.queries_to_load = self.queries_to_load

        ## Trigger queries
        self.trigger_queries(self.queries_to_load, days)
        
        ## Check query execution
        self.check_query_execution(self.queries_to_load)

        ## Load data
        df = self.extract_data(self.queries_to_load)     
        
        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------
    def trigger_queries(self, queries_to_load, days):
        for query in queries_to_load:
            if days == 'auto':
                day_val = get_missing_days_kpis(self.db_connector, query.metric_key, query.origin_key)
            else:
                day_val = days

            payload = {"paramsStr": "{\"params\":[{\"name\":\"Days\",\"value\":\"" + str(day_val) + "\"}]}"}

            query.last_run_id = self.client.trigger_query(query.query_id, payload)
            query.last_execution_loaded = False
            print(f"...triggered {query.origin_key} - {query.metric_key} with days: {days}. run_id: {query.last_run_id}")
            time.sleep(1)
    
    def check_query_execution(self, queries_to_load, wait=5):
        ## calculate time delta. if time delta is longer than 12 minutes, then end checking for finished queries
        start_time = time.time()

        while True:
            all_done = True
            for query in queries_to_load:
                if query.last_execution_loaded == False:
                    resp = self.client.check_query_execution(query.last_run_id)
                    if resp == False:
                        print(f"...wait for {query.origin_key} - {query.metric_key}.")
                        all_done = False
                        time.sleep(1)
                    elif resp == True:
                        query.last_execution_loaded = True
                        print(f'...done {query.origin_key} - {query.metric_key}')
                        time.sleep(1)
                    else:
                        print(f"issue with {query.origin_key} - {query.metric_key}")
                        query.last_execution_loaded = True
                        query.execution_error = True
                        print(resp)
                        time.sleep(1)
            if all_done == True:
                print("... ALL queries finished execution.")
                break
            else:
                current_duration = time.time() - start_time

                if current_duration > (7*60):
                    unfinished_queries = [x for x in queries_to_load if x.last_execution_loaded == False]
                    unfinished_str = [f"{x.origin_key}-{x.metric_key}" for x in unfinished_queries]
                    print(f"...queries not finished after 7 minutes. Ending loop. Following queries not finished: {unfinished_str}")
                    return
                else: 
                    time.sleep(wait)

    def extract_data(self, queries_to_load):
        dfMain = get_df_kpis()
        for query in queries_to_load:
            if query.last_execution_loaded == True:
                df = self.client.get_query_results(query.last_run_id)

                df = df.rename(columns={'day': 'date'})
                df['date'] = df['date'].apply(pd.to_datetime)
                df['date'] = df['date'].dt.date

                df = df.rename(columns={'val': 'value'})
                df['value'] = df['value'].astype(float)

                df['metric_key'] = query.metric_key
                df['origin_key'] = query.origin_key
                df.value.fillna(0, inplace=True)
                dfMain = pd.concat([dfMain,df])
                time.sleep(1)

        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain