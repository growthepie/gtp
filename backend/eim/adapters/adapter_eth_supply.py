import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import api_get_call, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterEthSupply(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ETH supply", adapter_params, db_connector)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
    """
    def extract(self, load_params:dict):
        self.load_type = load_params['load_type']
        days = load_params.get('days', 5)

        if self.load_type == 'extract_eth_supply':
            df = self.extract_eth_supply()
        elif self.load_type == 'supply_in_usd':                  
            df = self.get_supply_in_usd(days)
        elif self.load_type == 'issuance_rate':
            df = self.get_issuance_rate(days)
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")        

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        tbl_name = 'fact_eim'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    def extract_eth_supply(self):
        origin_key = 'ethereum'
        metric_key = 'eth_supply_eth'        
        url = 'https://ultrasound.money/api/v2/fees/supply-over-time'

        response_json = api_get_call(url, sleeper=10, retries=10)
        df = pd.DataFrame(response_json['since_burn'])

        ## rename timestamp to date and supply to value
        df.rename(columns={'timestamp':'date', 'supply':'value'}, inplace=True)

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.date

        df['metric_key'] = metric_key
        df['origin_key'] = origin_key

        df = df.drop_duplicates(subset=['date', 'metric_key', 'origin_key'])

        print(f"...{self.name} - loaded for {origin_key}. Shape: {df.shape}")
        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df
    
    def get_supply_in_usd(self, days):
        df = self.db_connector.get_values_in_usd_eim(['eth_supply_eth'], days)
        df = df[df['value'] != 0]
        df = df.dropna()
        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df
    
    def get_issuance_rate(self, days):
        df = self.db_connector.get_eth_issuance_rate(days)
        
        ## filter date to be after 2015-08-05
        df = df[df['date'] > datetime(2015,8,5).date()]
        
        df = df.dropna()
        df['metric_key'] = 'eth_issuance_rate'
        df['origin_key'] = 'ethereum'

        df.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df