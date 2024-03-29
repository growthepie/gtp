import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.chain_config import adapter_mapping
from src.queries.sql_queries import sql_queries
from src.misc.helper_functions import upsert_to_kpis, get_missing_days_kpis, get_missing_days_blockspace
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
        load_type:str - can be 'usd_to_eth' or 'eth_to_usd' or 'metrics' or 'blockspace' or 'profit'
        days:str - days of historical data that should be loaded, starting from today.
        origin_keys:list - list of origin_keys
        metric_keys:list - the metrics that should be loaded. If None, all available metrics will be loaded
    """
    def extract(self, load_params:dict):
        ## Set variables
        load_type = load_params['load_type']
        days = load_params['days']

        currency_dependent = load_params.get('currency_dependent', None)
        metric_keys = load_params.get('metric_keys', None)
        origin_keys = load_params.get('origin_keys', None)

        ## check if load_params['days_start'] exists and if so, overwrite days
        if 'days_start' in load_params:
            days_start = load_params['days_start']
        else:
            days_start = 1

        ## aggregation types
        if load_type == 'usd_to_eth': ## also make sure to add new metrics in db_connector
            raw_metrics = ['tvl', 'stables_mcap', 'fdv_usd']
            ## only keep metrics that are in raw_metrics and metric_keys
            if metric_keys is not None:
                metric_keys = [x for x in metric_keys if x in raw_metrics]
            else:
                metric_keys = raw_metrics
            df = self.db_connector.get_values_in_eth(metric_keys, days, origin_keys)

        elif load_type == 'eth_to_usd': ## also make sure to add new metrics in db_connector
            raw_metrics = ['fees_paid_eth', 'txcosts_median_eth', 'profit_eth', 'rent_paid_eth']
            ## only keep metrics that are in raw_metrics and metric_keys
            if metric_keys is not None:
                metric_keys = [x for x in metric_keys if x in raw_metrics]
            else:
                metric_keys = raw_metrics
            df = self.db_connector.get_values_in_usd(metric_keys, days, origin_keys)

        elif load_type == 'profit':
            ## chains to exclude from profit calculation: Offchain DA like IMX and Mantle
            exclude_chains = ['imx', 'mantle']
            df = self.db_connector.get_profit_in_eth(days, exclude_chains, origin_keys)

        elif load_type == 'fdv':
            df = self.db_connector.get_fdv_in_usd(days, origin_keys)

        elif load_type == 'metrics':        
            ## Prepare queries to load
            check_projects_to_load(sql_queries, origin_keys)

            if origin_keys is not None:
                self.queries_to_load = [x for x in sql_queries if x.origin_key in origin_keys]
            else:
                self.queries_to_load = sql_queries

            if currency_dependent is not None:
                if currency_dependent == True:
                    self.queries_to_load = [x for x in self.queries_to_load if x.currency_dependent == True]
                else:
                    self.queries_to_load = [x for x in self.queries_to_load if x.currency_dependent == False]

            if metric_keys is not None:
                self.queries_to_load = [x for x in self.queries_to_load if x.metric_key in metric_keys]
            else:
                ## remove queries that are have metric_key = 'profit_usd' since this should be triggered afterwards
                self.queries_to_load = [x for x in self.queries_to_load if x.metric_key != 'profit_usd']

            ## Load data
            df = self.extract_data_from_db(self.queries_to_load, days, days_start)

        elif load_type == 'blockspace':
            self.run_blockspace_queries(origin_keys, days)
            return None
        
        elif load_type == 'active_addresses_agg':
            self.run_active_addresses_agg(origin_keys, days)
            return None

        else:
            raise ValueError('load_type not supported')

        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        df.value.fillna(0, inplace=True)

        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    def extract_data_from_db(self, queries_to_load, days, days_start=1):
        query_list = []
        for query in queries_to_load:
            query_list.append(query.origin_key + ' - ' + query.metric_key)

        print(f"...loading data from db for {len(queries_to_load)} queries: {query_list}...")
        
        dfMain = pd.DataFrame()
        for query in queries_to_load:
            if days == 'auto':
                if query.metric_key == 'user_base_weekly':
                    day_val = 15
                elif query.origin_key == 'multi':
                    day_val = 40
                elif query.metric_key == 'maa':
                    day_val = 7
                elif query.metric_key == 'aa_last30d':
                    day_val = 3
                elif query.metric_key == 'aa_last7d':
                    day_val = 3
                else:
                    day_val = get_missing_days_kpis(self.db_connector, metric_key= query.metric_key, origin_key=query.origin_key)
            else:
                day_val = days
            
            if query.query_parameters is not None:
                query.update_query_parameters({'Days': day_val})
            
            if query.metric_key in ['aa_last30d', 'aa_last7d']:
                query.update_query_parameters({'Days_Start': days_start})
            
            print(f"...executing query: {query.metric_key} - {query.origin_key} with {query.query_parameters}")
            df = pd.read_sql(query.sql, self.db_connector.engine.connect())
            df['date'] = df['day'].apply(pd.to_datetime)
            df['date'] = df['date'].dt.date
            df.drop(['day'], axis=1, inplace=True)
            df.rename(columns= {'val':'value'}, inplace=True)
            if 'metric_key' not in df.columns:
                df['metric_key'] = query.metric_key
            if 'origin_key' not in df.columns:
                df['origin_key'] = query.origin_key
            df.value.fillna(0, inplace=True)

            dfMain = pd.concat([dfMain, df], ignore_index=True)
            print(f"Query loaded: {query.metric_key} {query.origin_key} with {day_val} days. DF shape: {df.shape}")
        return dfMain
    
    def run_blockspace_queries(self, origin_keys, days):
        if origin_keys is None:
            origin_keys = [chain.origin_key for chain in adapter_mapping if chain.aggregate_blockspace == True]
            print(f"...no specific origin_key found, aggregating blockspace for all chains: {origin_keys}...")

        for chain in origin_keys:
            if days == 'auto':
                days = get_missing_days_blockspace(self.db_connector, chain)
            else:
                days = days

            if chain == 'imx':
                print(f"...aggregating imx data for last {days} days...")
                df = self.db_connector.get_blockspace_imx(days)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting imx data . Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)
            
            else:
                ## aggregate contract data
                print(f"...aggregating contract data for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_contracts(chain, days)
                df.set_index(['address', 'date', 'origin_key'], inplace=True)

                print(f"...upserting contract data for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_contract_level', df)

                ## determine total usage
                print(f"...aggregating total usage for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_total(chain, days)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting total usage usage for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

                ## aggregate native transfers
                print(f"...aggregating native_transfers for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_native_transfers(chain, days)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting native_transfers for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

                ## aggregate contract deployments
                print(f"...aggregating smart_contract_deployments for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_contract_deplyments(chain, days)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting smart_contract_deployments for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

                ## aggregate inscriptions
                print(f"...aggregating inscriptions for {chain} and last {days} days...")
                df = self.db_connector.get_blockspace_inscriptions(chain, days)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting inscriptions for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

                # ALL below needs to be retriggerd when mapping changes (e.g. new addresses got labeled or new categories added etc.)
                ## aggregate by sub categories

                days_mapping = 5000
                print(f"...aggregating sub categories for {chain} and last {days_mapping} days...")
                df = self.db_connector.get_blockspace_sub_categories(chain, days_mapping)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting sub categories for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

                ## determine unlabeled usage
                print(f"...aggregating unlabeled usage for {chain} and last {days_mapping} days...")
                df = self.db_connector.get_blockspace_unlabeled(chain, days_mapping)
                df.set_index(['date', 'sub_category_key' ,'origin_key'], inplace=True)

                print(f"...upserting unlabeled usage for {chain}. Total rows: {df.shape[0]}...")
                self.db_connector.upsert_table('blockspace_fact_sub_category_level', df)

            print(F"Finished loading blockspace queries for {chain}")
        print(F"Finished loading blockspace for all chains")

    def run_active_addresses_agg(self, origin_keys, days):
        if origin_keys is None:
            origin_keys = [chain.origin_key for chain in adapter_mapping if chain.aggregate_addresses == True]
            print(f"...no specific origin_key found, aggregating blockspace for all chains: {origin_keys}...")

        for origin_key in origin_keys:
            if days == 'auto':
                days = 7
            else:
                days = int(days)

            print(f"...aggregating active addresses data for {origin_key} and last {days} days...")
            df = self.db_connector.get_unique_addresses(origin_key, days)
            ## drop rows with null addresses (i.e. Loopring)
            df = df.dropna(subset=['address'])
            df.set_index(['address', 'date', 'origin_key'], inplace=True)

            print(f"...upserting active addresses data for {origin_key}. Total rows: {df.shape[0]}...")
            self.db_connector.upsert_table('fact_active_addresses', df)
