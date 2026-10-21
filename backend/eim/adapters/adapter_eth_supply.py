import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import api_get_call
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
        elif self.load_type == 'extract_eth_supply_hourly':
            df = self.extract_eth_supply_hourly()
        elif self.load_type == 'extract_eth_staking_hourly':
            df = self.extract_eth_staking_hourly(load_params.get('range', '24h'))
        elif self.load_type == 'supply_in_usd':                  
            df = self.get_supply_in_usd(days)
        elif self.load_type == 'issuance_rate':
            df = self.get_issuance_rate(days)
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")        

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        tbl_name = 'fact_kpis_granular' if self.load_type in ['extract_eth_supply_hourly', 'extract_eth_staking_hourly'] else 'eim_fact'
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
    
    ## hourly supply for the last 30 days (ultrasound 'd30' series has ~1 datapoint per hour), last datapoint per hour is kept
    def extract_eth_supply_hourly(self):
        url = 'https://ultrasound.money/api/v2/fees/supply-over-time'

        response_json = api_get_call(url, sleeper=10, retries=10)
        df = pd.DataFrame(response_json['d30'])

        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None).dt.floor('h')
        df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
        df.rename(columns={'supply':'value'}, inplace=True)

        df['metric_key'] = 'eth_supply_eth'
        df['origin_key'] = 'ethereum'
        df['granularity'] = 'hourly'

        print(f"...{self.name} - hourly supply loaded. Shape: {df.shape}")
        df.set_index(['metric_key', 'origin_key', 'timestamp', 'granularity'], inplace=True)
        return df

    ## hourly staking data from ethsupply.fyi:
    ##   eth_staked_eth: actual balance of active validators, last datapoint per hour
    ##   staking_rewards_cl_eth: net CL rewards (issuance - consensus penalties) per hour, normalized via avg per epoch * 9.375 epochs/hour
    ## range '24h' has one datapoint per epoch, '7d' every 10 epochs, '30d' every 30 epochs (longer ranges only meant for backfills)
    def extract_eth_staking_hourly(self, range:str='24h'):
        url = f'https://ethsupply.fyi/api/history?range={range}'

        response_json = api_get_call(url, sleeper=10, retries=5)
        if not response_json or not response_json.get('staking') or not response_json.get('slots'):
            raise ValueError(f"No staking data returned from {url}")
        interval_epochs = int(response_json['intervalEpochs'])

        df_staked = pd.DataFrame(response_json['staking'])[['timestamp', 'activeBalanceGwei']]
        df_staked['timestamp'] = pd.to_datetime(df_staked['timestamp'].astype(int), unit='s').dt.floor('h')
        df_staked['value'] = df_staked['activeBalanceGwei'].astype(float) / 1e9
        df_staked = df_staked.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')[['timestamp', 'value']]
        df_staked['metric_key'] = 'eth_staked_eth'

        df_cl = pd.DataFrame(response_json['slots'])[['timestamp', 'issuanceWei', 'consensusPenaltiesWei']]
        df_cl['timestamp'] = pd.to_datetime(df_cl['timestamp'].astype(int), unit='s').dt.floor('h')
        df_cl['value'] = (df_cl['issuanceWei'].astype(float) - df_cl['consensusPenaltiesWei'].astype(float)) / 1e18 / interval_epochs
        df_cl = df_cl.groupby('timestamp', as_index=False)['value'].mean()
        df_cl['value'] = df_cl['value'] * 3600 / (12 * 32)
        df_cl['metric_key'] = 'staking_rewards_cl_eth'

        df = pd.concat([df_staked, df_cl])
        df['origin_key'] = 'ethereum'
        df['granularity'] = 'hourly'

        print(f"...{self.name} - hourly staking data loaded. Shape: {df.shape}")
        df.set_index(['metric_key', 'origin_key', 'timestamp', 'granularity'], inplace=True)
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