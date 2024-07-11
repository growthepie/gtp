import os
import simplejson as json
import datetime
import pandas as pd
import numpy as np
import random
from datetime import timedelta, datetime

from src.chain_config import adapter_mapping, adapter_multi_mapping
from src.misc.helper_functions import upload_json_to_cf_s3, upload_parquet_to_cf_s3, db_addresses_to_checksummed_addresses, fix_dict_nan
from src.misc.glo_prep import Glo

import warnings

# Suppress specific FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def merge_dicts(default, custom):
    merged = default.copy()
    for key, value in custom.items():
        if isinstance(value, dict) and key in merged:
            merged[key] = merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged

class JSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector, api_version):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector

        ## Decimals: only relevant if value isn't aggregated
        ## When aggregated (starting >1k), we always show 2 decimals
        ## in case of ETH and decimals >6, show Gwei
        ## prefix and suffix should also show in axis
        self.units = {
            'value': {
                'currency': False,
                'prefix': None,
                'suffix': None,
                'decimals': 0,
                'decimals_tooltip': 0,
                'agg': True,
                'agg_tooltip': True,
            },
            'usd': {
                'currency': True,
                'prefix': '$',
                'suffix': None,
                'decimals': 2,
                'decimals_tooltip': 2,
                'agg': True, 
                'agg_tooltip': False,
            },
            'eth': {
                'currency': True,
                'prefix': 'Ξ',
                'suffix': None,
                'decimals': 2,
                'decimals_tooltip': 2,
                'agg': True,
                'agg_tooltip': False,
            },
        }

        self.metrics = {
            'tvl': {
                'name': 'Total Value Locked',
                'fundamental': True,
                'metric_keys': ['tvl', 'tvl_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}, 
                    'eth': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': False, ##7d rolling average
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': False
            }
            ,'txcount': {
                'name': 'Transaction Count',
                'fundamental': True,
                'metric_keys': ['txcount'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False
            }
            ,'daa': {
                'name': 'Active Addresses',
                'fundamental': True,
                'metric_keys': ['daa'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'maa',
                'max_date_fill' : False,
                'ranking_bubble': True
            }
            ,'stables_mcap': {
                'name': 'Stablecoin Market Cap',
                'fundamental': True,
                'metric_keys': ['stables_mcap', 'stables_mcap_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}, 
                    'eth': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': False,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True
            }
            ,'fees': {
                'name': 'Fees Paid by Users',
                'fundamental': True,
                'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': True
            }
            ,'rent_paid': {
                'name': 'Rent Paid to L1',
                'fundamental': True,
                'metric_keys': ['rent_paid_usd', 'rent_paid_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : True,
                'ranking_bubble': False
            }
            ,'profit': {
                'name': 'Onchain Profit',
                'fundamental': True,
                'metric_keys': ['profit_usd', 'profit_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : True,
                'ranking_bubble': True
            }
            ,'txcosts': {
                'name': 'Transaction Costs',
                'fundamental': True,
                'metric_keys': ['txcosts_median_usd', 'txcosts_median_eth'],
                'units': {
                    'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'weighted_mean',
                'monthly_agg': 'avg',
                'max_date_fill' : True,
                'ranking_bubble': True
            }
            ,'fdv': {
                'name': 'Fully Diluted Valuation',
                'fundamental': True,
                'metric_keys': ['fdv_usd', 'fdv_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True
            }
            ,'market_cap': {
                'name': 'Market Cap',
                'fundamental': True,
                'metric_keys': ['market_cap_usd', 'market_cap_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': False
            }
            ,'throughput': {
                'name': 'Throughput',
                'fundamental': True,
                'metric_keys': ['gas_per_second'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False, 'suffix': 'Mgas/s'},
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True
            }

            ## Non fundamental metrics
            ,'total_blob_size': {
                'name': 'Total Blob Size',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['total_blob_size_bytes'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False, 'agg': False, 'suffix': 'Bytes'},
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False
            }

            ,'total_blob_fees': {
                'name': 'Total Blob Fees',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['total_blobs_usd', 'total_blobs_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False
            }

            ,'costs': {
                'name': 'Costs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_total_usd', 'costs_total_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False
            }
        }

        self.fees_types = {
            'txcosts_median' : {
                'name': 'Median Fee',
                'name_short': 'Median Fee',
                'metric_keys': ['txcosts_median_eth'],
                'units': {
                    'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'category': 'Fees',
                'currency': True,
                'priority': 1,
                'invert_normalization': False
            }
            ,'txcosts_native_median' : {
                'name': 'Transfer ETH Fee',
                'name_short': 'Transfer ETH',
                'metric_keys': ['txcosts_native_median_eth'],
                'units': {
                    'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'category': 'Fees',
                'currency': True,
                'priority': 2,
                'invert_normalization': False
            }
            , 'tps' : {
                'name': 'Transactions per Second',
                'name_short': 'TPS',
                'metric_keys': ['txcount'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False},
                },
                'category': 'Activity',
                'currency': False,
                'priority': 3,
                'invert_normalization': True
            }
            , 'throughput' : {
                'name': 'Throughput',
                'name_short': 'Throughput',
                'metric_keys': ['gas_per_second'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False, 'suffix': 'Mgas/s'},
                },
                'category': 'Activity',
                'currency': False,
                'priority': 4,
                'invert_normalization': True
            }
            , 'txcosts_swap' : {
                'name': 'Swap Fee',
                'name_short': 'Swap Fee',
                'metric_keys': ['txcosts_swap_eth'],
                'units': {
                    'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'category': 'Fees',
                'currency': True,
                'priority': 5,
                'invert_normalization': False
            }
            ,'txcosts_avg' : {
                'name': 'Average Fee',
                'name_short': 'Average Fee',
                'metric_keys': ['txcosts_avg_eth'],
                'units': {
                    'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'category': 'Fees',
                'currency': True,
                'priority': 6,
                'invert_normalization': False
            }          
        }

        ## mapping of timeframes to granularity
        self.fees_timespans = {
            '24hrs' : {'granularity': '10_min', 'filter_days': 1, 'tps_divisor': 60*10},
            '7d' : {'granularity': 'hourly', 'filter_days': 7, 'tps_divisor': 60*60},
            '30d' : {'granularity': '4_hours', 'filter_days': 30, 'tps_divisor': 60*60*4},
            '180d' : {'granularity': 'daily', 'filter_days': 180, 'tps_divisor': 60*60*24},
            }
        
        for metric_key, metric_value in self.metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.fees_types.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        #append all values of metric_keys in metrics dict to a list
        self.metrics_list = [item for sublist in [self.metrics[metric]['metric_keys'] for metric in self.metrics] for item in sublist]
        #concat all values of metrics_list to a string and add apostrophes around each value
        self.metrics_string = "'" + "','".join(self.metrics_list) + "'"

        #append all keys of chains dict to a list
        self.chains_list = [x.origin_key for x in adapter_mapping]
        #concat all values of chains_list to a string and add apostrophes around each value
        self.chains_string = "'" + "','".join(self.chains_list) + "'"
        #only chains that are in the api output
        self.chains_list_in_api = [x.origin_key for x in adapter_mapping if x.in_api == True]
        #only chains that are in the api output and deployment is "PROD"
        self.chains_list_in_api_prod = [x.origin_key for x in adapter_mapping if x.in_api == True and x.deployment == "PROD"]
        #only chains that are in the api output and deployment is "PROD" and in_labels_api is True
        self.chains_list_in_api_labels = [x.origin_key for x in adapter_mapping if x.in_api == True and x.in_labels_api == True]

        ## all feest metrics keys
        self.fees_list = [item for sublist in [self.fees_types[metric]['metric_keys'] for metric in self.fees_types] for item in sublist]

    
    ###### CHAIN DETAILS AND METRIC DETAILS METHODS ########

    def df_rename(self, df, metric_id, col_name_removal=False):
        # print(f'called df_rename for {metric_id}')
        # print(df.columns.to_list())
        if col_name_removal:
            df.columns.name = None

        if 'usd' in self.metrics[metric_id]['units'] or 'eth' in self.metrics[metric_id]['units']:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                elif col.endswith('_eth'):
                    df.rename(columns={col: 'eth'}, inplace=True)
                else:
                    df.rename(columns={col: 'usd'}, inplace=True)

            if 'unix' in df.columns.to_list():
                df = df[['unix', 'usd', 'eth']]
            else:
                df = df[['usd', 'eth']]
        else:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                else:
                    df.rename(columns={col: 'value'}, inplace=True)
        return df

    # Function to trim leading zeros for each group
    def trim_leading_zeros(self, group):
        first_non_zero_index = group['value'].ne(0).idxmax()  # Find first non-zero index in each group
        return group.loc[first_non_zero_index:]  # Return the DataFrame slice starting from this index
    
    def get_ranking(self, df, metric_id, origin_key):
        mks = self.metrics[metric_id]['metric_keys']
        ## remove elements in mks list that end with _eth
        mks = [x for x in mks if not x.endswith('_eth')]

        ## First filter down to metric
        df_tmp = df.loc[(df.metric_key.isin(mks)), ["origin_key", "value", "metric_key", "date"]]

        ## then max date of this metric per origin_key
        #df_tmp = df_tmp.loc[(df_tmp.date == df_tmp.date.max()), ["origin_key", "value", "metric_key", "date"]]
        df_tmp = df_tmp.loc[df_tmp.groupby("origin_key")["date"].idxmax()]

        ## asign rank based on order (descending order if metric_key is not 'txcosts')
        if metric_id != 'txcosts':
            df_tmp['rank'] = df_tmp['value'].rank(ascending=False, method='first')
        else:
            df_tmp['rank'] = df_tmp['value'].rank(ascending=True, method='first')

        ## get rank for origin_key
        if df_tmp.loc[df_tmp.origin_key == origin_key].shape[0] > 0:
            rank = df_tmp.loc[df_tmp.origin_key == origin_key, 'rank'].values[0]
            rank_max = df_tmp['rank'].max()

            print(f"...rank for {origin_key} and {metric_id} is {int(rank)} out of {int(rank_max)}")
            return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2)}    
        else:
            print(f"...no rank for {origin_key} and {metric_id}")
            return {'rank': None, 'out_of': None, 'color_scale': None}         


    # this method returns a list of lists with the unix timestamp and all associated values for a certain metric_id and chain_id
    def generate_daily_list(self, df, metric_id, origin_key, start_date = None):
        ##print(f'called generate int for {metric_id} and {origin_key}')
        mks = self.metrics[metric_id]['metric_keys']
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["unix", "value", "metric_key", "date"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]
        
        max_date = df_tmp['date'].max()
        max_date = pd.to_datetime(max_date).replace(tzinfo=None)
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.date()

        ## if max_date_fill is True, fill missing rows until yesterday with 0
        if self.metrics[metric_id]['max_date_fill']:
            #check if max_date is yesterday
            if max_date.date() != yesterday:
                print(f"max_date in df for {mks} is {max_date}. Will fill missing rows until {yesterday} with None.")

                date_range = pd.date_range(start=max_date + timedelta(days=1), end=yesterday, freq='D')

                for mkey in mks:
                    new_data = {'date': date_range, 'value': [0] * len(date_range), 'metric_key': mkey}
                    new_df = pd.DataFrame(new_data)
                    new_df['unix'] = new_df['date'].apply(lambda x: x.timestamp() * 1000)

                    df_tmp = pd.concat([df_tmp, new_df], ignore_index=True)

        ## trime leading zeros
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp = df_tmp.groupby('metric_key').apply(self.trim_leading_zeros).reset_index(drop=True)

        df_tmp.drop(columns=['date'], inplace=True)
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        
        df_tmp = self.df_rename(df_tmp, metric_id, True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(self.metrics[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(self.metrics[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        return mk_list_int, df_tmp.columns.to_list()
    
    # this method returns a list of lists with the unix timestamp (first day of month) and all associated values for a certain metric_id and chain_id
    def generate_monthly_list(self, df, metric_id, origin_key):
        ##print(f'called generate int for {metric_id} and {chain_id}')
        mks = self.metrics[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'maa'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "unix", "value", "metric_key"]]
        ## create monthly averages on value and min on unix column
        df_tmp['date'] = df_tmp['date'].dt.tz_convert(None) ## get rid of timezone in order to avoid warnings

        ## replace earliest date with first day of month (in unix) in unix column
        df_tmp['unix'] = (df_tmp['date'].dt.to_period("M").dt.start_time).astype(np.int64) // 10**6

        if self.metrics[metric_id]['monthly_agg'] == 'sum':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'sum', 'unix': 'min'}).reset_index()
        elif self.metrics[metric_id]['monthly_agg'] == 'avg':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'mean', 'unix': 'min'}).reset_index()
        elif self.metrics[metric_id]['monthly_agg'] == 'maa':
            pass ## no aggregation necessary
        else:
            raise NotImplementedError(f"monthly_agg {self.metrics[metric_id]['monthly_agg']} not implemented")

        ## drop column date
        df_tmp = df_tmp.drop(columns=['date'])
        ## metric_key to column
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)

        df_tmp = self.df_rename(df_tmp, metric_id, True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(self.metrics[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(self.metrics[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")

        return mk_list_int, df_tmp.columns.to_list()

    def generate_fees_list(self, df, metric, origin_key, granularity, timespan, eth_price, max_ts_all = None, normalize = False):
        metric_key = self.fees_types[metric]['metric_keys'][0]
        ## filter df to granularity = 'hourly' and metric_key = metric
        df = df[(df.granularity == granularity) & (df.metric_key == metric_key) & (df.origin_key == origin_key)]
        
        ## Create new rows for missing timestamps
        if max_ts_all is not None:
            ## certain metric_key/origin_key combinations are empty (i.e. Starknet native transfer fees)
            if df.empty:
                print(f"df is empty for {metric_key} and {origin_key}. Will skip.")
            else:
                max_ts = df['unix'].max()
                #check if filtered max_ts is the same as max_ts_all
                if max_ts != max_ts_all:
                    print(f"max_ts in df (shape {df.shape}) for {metric_key} and {origin_key} is {max_ts}. Will fill missing rows until {max_ts_all} with None.")

                    start_date = pd.to_datetime(max_ts, unit='ms', utc=True)
                    end_date = pd.to_datetime(max_ts_all, unit='ms', utc=True)

                    print(f"start_date: {start_date}, end_date: {end_date} for {metric_key} and {origin_key}")

                    if granularity == 'hourly':
                        date_range = pd.date_range(start=start_date, end=end_date, freq='H')
                    elif granularity == '10_min':
                        date_range = pd.date_range(start=start_date, end=end_date, freq='10T')
                    else:
                        raise NotImplementedError(f"Granularity {granularity} not implemented")

                    new_data = {'timestamp': date_range, 'value': [None] * len(date_range)}
                    new_df = pd.DataFrame(new_data)
                    new_df['unix'] = new_df['timestamp'].apply(lambda x: x.timestamp() * 1000)
                    ## drop row with smallest timestamp
                    new_df = new_df[new_df['unix'] != max_ts]

                    df = pd.concat([df, new_df], ignore_index=True)

        ## order df_tmp by timestamp desc
        df = df.sort_values(by='timestamp', ascending=False)
        ## only keep columns unix, value_usd
        df = df[['unix', 'value']]

        if self.fees_types[metric]['currency']:
            main_col = 'value_eth'
            ## calculate value_usd by multiplying value with eth_price
            df['value_usd'] = df['value'] * eth_price
            df.rename(columns={'value': main_col}, inplace=True)
            ## order columns in df unix, value_eth, value_usd
            df = df[['unix', main_col, 'value_usd']]
        elif metric == 'tps':
            df['value'] = df['value'] / self.fees_timespans[timespan]['tps_divisor']
            main_col = 'value'
        elif metric == 'throughput':
            df['value'] = df['value'] / (1000 * 1000)
            main_col = 'value'
        else:
            main_col = 'value'

        if normalize:
            ## get max value and min value
            max_value = df[main_col].max()
            min_value = df[main_col].min()
            ## create new column 'normalized' with normalized values between 0 and 1
            df['normalized'] = (df[main_col] - min_value) / (max_value - min_value)

            if self.fees_types[metric]['invert_normalization']:
                df['normalized'] = 1 - df['normalized']

            mk_list = df.values.tolist()
            if self.fees_types[metric]['currency']:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## eth value
                        round(i[2], 4), ## usd value
                        round(i[3], 2) ## normalized value
                    ] for i in mk_list]
            else:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## value
                        round(i[2], 2) ## normalized value
                    ] for i in mk_list]
        else:
            mk_list = df.values.tolist()
            if self.fees_types[metric]['currency']:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1], ## eth value
                        round(i[2], 4) ## usd value
                    ] for i in mk_list]
            else:
                mk_list_int = [
                    [
                        int(i[0]), ## unix timestamp
                        i[1] ## value
                    ] for i in mk_list]
                

        return mk_list_int, df.columns.to_list()

    ## create 7d rolling average over a list of lists where the first element is the date and the second element is the value (necessary for daily_avg field)
    def create_7d_rolling_avg(self, list_of_lists):
        avg_list = []
        if len(list_of_lists[0]) == 2: ## all non USD metrics e.g. txcount
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1]])
                else:
                    avg = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    ## round to 2 decimals
                    avg = round(avg, 2)
                    avg_list.append([list_of_lists[i][0], avg])
        else: ## all USD metrics e.g. fees that have USD and ETH values
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1], list_of_lists[i][2]] )
                else:
                    avg_1 = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    avg_2 = (list_of_lists[i][2] + list_of_lists[i-1][2] + list_of_lists[i-2][2] + list_of_lists[i-3][2] + list_of_lists[i-4][2] + list_of_lists[i-5][2] + list_of_lists[i-6][2]) / 7
    
                    avg_list.append([list_of_lists[i][0], avg_1, avg_2])
        
        return avg_list


    def download_data(self, chains_string, metrics_string):
        exec_string = f"""
            SELECT 
                kpi.metric_key, 
                kpi.origin_key as origin_key, 
                kpi."date", 
                kpi.value
            FROM public.fact_kpis kpi
            where kpi.origin_key in ({chains_string})
                and kpi.metric_key in ({metrics_string})
                and kpi."date" >= '2021-01-01'
                and kpi."date" < date_trunc('day', now())
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        #df.drop(columns=['date'], inplace=True)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df
    
    def download_data_fees(self, metric_keys):
        exec_string = f"""
            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '181 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = 'daily'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '31 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = '4_hours'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '8 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = 'hourly'

            UNION ALL

            SELECT *
            FROM public.fact_kpis_granular kpi
            where kpi."timestamp" >= CURRENT_DATE - INTERVAL '2 days'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
                and granularity = '10_min'
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['timestamp'].apply(lambda x: x.timestamp() * 1000)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df

    def create_changes_dict(self, df, metric_id, origin_key):
        #print(f'called create_changes_dict for {metric_id} and {origin_key}')
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(self.metrics[metric_id]['metric_keys'])), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        changes_dict = {
                        'types': [],
                        '1d': [],
                        '7d': [],
                        '30d': [],
                        '90d': [],
                        '180d': [],
                        '365d': []
                    }

        for mk in self.metrics[metric_id]['metric_keys']:
            #print(mk)
            cur_val = df_tmp[mk].iloc[0]
            changes = [1,7,30,90,180,365]
            for change in changes:
                if df_tmp[mk].shape[0] <= change:
                    change_val = None
                elif df_tmp[mk].iloc[change] == 0:
                    change_val = None
                else:
                    prev_val = df_tmp[mk].iloc[change]
                    if (prev_val < 0) or (cur_val < 0) or (prev_val == 0):
                        change_val = None
                    else:
                        change_val = (cur_val - prev_val) / prev_val
                        change_val = round(change_val, 4)
                        if change_val > 100:
                            change_val = 99.99
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    def create_changes_dict_monthly(self, df, metric_id, origin_key):
        #print(f'called create_changes_dict for {metric_id} and {origin_key}')
        mks = self.metrics[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'aa_last30d'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        changes_dict = {
                        'types': [],
                        '30d': [],
                        '90d': [],
                        '180d': [],
                        '365d': [],
                    }

        for mk in mks:
            #print(mk)
            if self.metrics[metric_id]['monthly_agg'] == 'sum':
                cur_val = df_tmp[mk].iloc[0:29].sum()
            elif self.metrics[metric_id]['monthly_agg'] == 'avg':
                cur_val = df_tmp[mk].iloc[0:29].mean()
            elif self.metrics[metric_id]['monthly_agg'] == 'maa':
                cur_val = df_tmp[mk].iloc[0]
            else:
                raise NotImplementedError(f"monthly_agg {self.metrics[metric_id]['monthly_agg']} not implemented")
            
            changes = [30,90,180,365]
            for change in changes:
                if df_tmp[mk].shape[0] <= change:
                    change_val = None
                else:
                    if self.metrics[metric_id]['monthly_agg'] == 'sum':
                        prev_val = df_tmp[mk].iloc[change:change+29].sum()
                    elif self.metrics[metric_id]['monthly_agg'] == 'avg':
                        prev_val = df_tmp[mk].iloc[change:change+29].mean()
                    elif self.metrics[metric_id]['monthly_agg'] == 'maa':
                        prev_val = df_tmp[mk].iloc[change]
                    else:
                        raise NotImplementedError(f"monthly_agg {self.metrics[metric_id]['monthly_agg']} not implemented")
                    
                    if (prev_val < 0) or (cur_val < 0) or (prev_val == 0):
                        change_val = None
                    else:
                        change_val = (cur_val - prev_val) / prev_val
                        change_val = round(change_val, 4)
                        if change_val >= 100:
                            change_val = 99.99
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    ## this function takes a dataframe and a metric_id and origin_key as input and returns a value that aggregates the last 30 days
    def value_last_30d(self, df, metric_id, origin_key):     
        mks = self.metrics[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'aa_last30d'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        df_tmp = self.df_rename(df_tmp, metric_id, True)

        if self.metrics[metric_id]['monthly_agg'] == 'sum':
            val = df_tmp.iloc[0:29].sum()
        elif self.metrics[metric_id]['monthly_agg'] == 'avg':
            val = df_tmp.iloc[0:29].mean()
        elif self.metrics[metric_id]['monthly_agg'] == 'maa':
            val = df_tmp.iloc[0]

        val_dict = {
            'types': val.keys().to_list(),
            'data': val.to_list()
        }

        return val_dict

    def get_all_data(self):
        ## Load all data from database
        chain_user_list = self.chains_list_in_api + ['multiple', 'celestia']
        metric_user_list = self.metrics_list + ['user_base_daily', 'user_base_weekly', 'user_base_monthly', 'waa', 'maa', 'aa_last30d', 'aa_last7d', 
                                                'cca_last7d_exclusive', 'costs_total_eth', 'costs_total_usd', 'costs_l1_eth', 'costs_l1_usd', 
                                                'costs_blobs_eth', 'costs_blobs_usd', 'blob_size_bytes']

        chain_user_string = "'" + "','".join(chain_user_list) + "'"
        metrics_user_string = "'" + "','".join(metric_user_list) + "'"

        df = self.download_data(chain_user_string, metrics_user_string)

        ## divide value by 1000000 where metric_key is gas_per_second --> mgas/s
        df.loc[df['metric_key'] == 'gas_per_second', 'value'] = df['value'] / 1000000
        return df
    
    def get_data_fees(self):
        df = self.download_data_fees(self.fees_list)
        return df
    
    ##### LANDING PAGE METHODS #####
    ## This method calculates the total l2 users or the users for a specific chain
    def end_of_month(self, dt):
        todays_month = dt.month
        tomorrows_month = (dt + datetime.timedelta(days=1)).month
        return tomorrows_month != todays_month

    def chain_users(self, df, aggregation, origin_key, comparison=False):
        ### filter the dataframes accordingly
        ## and get the correct max date (latest date with user info for the chain) so that we can make sure that we don't have any half weeks/months
        if origin_key == 'all_l2s':
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='user_base_'+aggregation)]
        elif origin_key == 'ethereum' and aggregation == 'daily':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='daa')]
        elif origin_key == 'ethereum' and aggregation == 'weekly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='waa')]
        elif origin_key == 'ethereum' and aggregation == 'monthly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='maa')]
        else:
            df_tmp = df.loc[(df.origin_key == origin_key) & (df.metric_key=='user_base_'+aggregation)]

        df_tmp = df_tmp[['date', 'value']]
        df_tmp = df_tmp.groupby(pd.Grouper(key='date')).sum().reset_index()

        if comparison == False:
            ## only keep latest date
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        else:
            ## drop latest date and keep the one before
            df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]

        users = df_tmp.value.sum()    
        return users

    def create_chain_users_comparison_value(self, df, aggregation, origin_key):
        val = (self.chain_users(df, aggregation, origin_key) - self.chain_users(df, aggregation, origin_key, comparison=True)) / self.chain_users(df, aggregation, origin_key, comparison=True)
        return round(val, 4)

    ## This method divdes the total l2 users by the total users of the Ethereum chain 
    def l2_user_share(self, df, aggregation, comparison=False):
        l2_user_share = self.chain_users(df, aggregation, 'all_l2s', comparison) / self.chain_users(df, aggregation, 'ethereum', comparison)
        return round(l2_user_share,4)
    
    def create_l2_user_share_comparison_value(self, df, aggregation):
        current = self.l2_user_share(df, aggregation)
        previous = self.l2_user_share(df, aggregation, comparison=True)
        val = (current - previous) / previous 
        return round(val,4)
    
    def cross_chain_users(self, df, comparison=False):
        ## filter df down to origin_key = 'multiple' and metric_key = 'user_base_weekly
        df_tmp = df.loc[(df.origin_key == 'multiple') & (df.metric_key == 'user_base_weekly')]
        df_tmp = df_tmp[['date', 'value']]

        if comparison == False:
            ## filter df to latest date
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        else:
            ## drop latest date and keep the one before
            df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        return int(df_tmp['value'].values[0])
    
    def create_cross_chain_users_comparison_value(self, df):
        val = (self.cross_chain_users(df) - self.cross_chain_users(df, comparison=True)) / self.cross_chain_users(df, comparison=True)
        return round(val, 4)
    
    def get_aa_last7d(self, df, origin_key):
        if origin_key == 'all':
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='aa_last7d')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
            df_tmp = df_tmp.groupby(pd.Grouper(key='date')).sum().reset_index()
        else:
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='aa_last7d')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        return int(df_tmp['value'].values[0])
    
    def get_cross_chain_activity(self, df, chain):
        if chain.aggregate_addresses == False or chain.origin_key == 'starknet':
            print(f'...cross_chain_activity for {chain.origin_key} is not calculated because aggregate_addresses is set to False.')
            return 0
        else:
            origin_key = chain.origin_key
            ## calculate the exclusive users for the chain
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='cca_last7d_exclusive')]
            df_tmp = df_tmp[['date', 'value']]
            df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
            exclusive_ussers = int(df_tmp['value'].values[0])

            ## calculate the cross_chain activity percentage
            total_users = self.get_aa_last7d(df, origin_key)
            cross_chain_activity = 1 - (exclusive_ussers / total_users)

            if cross_chain_activity < 0:
                cross_chain_activity = 0
            if cross_chain_activity > 1:
                cross_chain_activity = 1
            #print(f'cross_chain_activity for {origin_key} is {cross_chain_activity} based on {exclusive_ussers} exclusive users and {total_users} total users.')

            return round(cross_chain_activity, 4)

    def get_landing_table_dict(self, df):
        chains_dict = {}
        all_users = self.get_aa_last7d(df, 'all')
        for chain in adapter_mapping:
            if chain.in_api == True and chain.origin_key != 'ethereum':
                chains_dict[chain.origin_key] = {
                    "chain_name": chain.name,
                    "technology": chain.technology,
                    "purpose": chain.purpose,
                    "users": self.get_aa_last7d(df, chain.origin_key),
                    "user_share": round(self.get_aa_last7d(df, chain.origin_key)/all_users,4),
                    "cross_chain_activity": self.get_cross_chain_activity(df, chain)
                }
        
        return chains_dict

    def get_filtered_df(self, df, aggregation, origin_key):
        chains_list_no_eth = self.chains_list.copy()
        chains_list_no_eth.remove('ethereum')
        chains_list_no_eth.append('multiple')
        
        if origin_key == 'all_l2s':
            df_tmp = df.loc[(df.origin_key.isin(chains_list_no_eth)) & (df.metric_key=='user_base_'+aggregation)]
            df_tmp = df_tmp[['unix', 'value']]
            df_tmp = df_tmp.groupby(pd.Grouper(key='unix')).sum().reset_index()
        elif origin_key == 'ethereum' and aggregation == 'daily':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='daa')]
            df_tmp = df_tmp[['unix', 'value']]
        elif origin_key == 'ethereum' and aggregation == 'weekly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='waa')]
            df_tmp = df_tmp[['unix', 'value']]
        elif origin_key == 'ethereum' and aggregation == 'monthly':
            df_tmp = df.loc[(df.origin_key=='ethereum') & (df.metric_key=='maa')]    
            df_tmp = df_tmp[['unix', 'value']]
        else:
            df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key=='user_base_'+aggregation)]	
            df_tmp = df_tmp[['unix', 'value']]
        
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)	
        return df_tmp

    def create_userbase_list_of_lists(self, df, aggregation, origin_key):
        df_tmp = self.get_filtered_df(df, aggregation, origin_key)
        mk_list = df_tmp.values.tolist() ## creates a list of lists
        mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        return mk_list_int

    def generate_userbase_dict(self, df, chain, aggregation):
        total_l2_users = self.chain_users(df, aggregation, 'all_l2s')
        if chain.origin_key in ['ethereum', 'all_l2s']:
            user_share = 0
        else:
            user_share = round(self.chain_users(df, aggregation, chain.origin_key) / total_l2_users, 4)
        dict = {
                    "chain_name": chain.name,
                    "technology": chain.technology,
                    "purpose": chain.purpose,
                    "user_share": user_share,
                    "data": {
                        "types": [
                            "unix",
                            "value"
                        ],
                        "data": self.create_userbase_list_of_lists(df, aggregation, chain.origin_key)
                        }	
                }
        return dict

    def generate_chains_userbase_dict(self, df, aggregation):             
        chains_dict = {} 
        # filter df to date >= 2021-09-01
        df = df.loc[df.date >= '2021-09-01']

        for chain in adapter_multi_mapping:
            chains_dict[chain.origin_key] = self.generate_userbase_dict(df, chain, aggregation)
        return chains_dict
    
    ## This method generates a dict containing aggregate daily values for all_l2s (all chains except Ethereum) for a specific metric_id
    def generate_all_l2s_metric_dict(self, df, metric_id, rolling_avg=False):
        metric = self.metrics[metric_id]
        mks = metric['metric_keys']

        # filter df for all_l2s (all chains except Ethereum and chains that aren't included in the API)
        chain_keys = [chain.origin_key for chain in adapter_mapping if chain.in_api == True]
        df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(chain_keys))]

        # filter df _tmp by date so that date is greather than 2 years ago
        df_tmp = df_tmp.loc[df_tmp.date >= (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')]  
        
        # group by unix and metric_key and sum up the values or calculate the mean (depending on the metric)
        if metric['all_l2s_aggregate'] == 'sum':
            df_tmp = df_tmp.groupby(['date', 'unix', 'metric_key']).sum().reset_index()
        elif metric['all_l2s_aggregate'] == 'weighted_mean':
            ## calculate weighted mean based on txcount of each chain
            # get txcount of each chain
            df_txcount = df.loc[(df.origin_key!='ethereum') & (df.metric_key=='txcount')]

            # merge txcount df with df_tmp
            df_tmp = df_tmp.merge(df_txcount[['date', 'origin_key', 'value']], on=['date', 'origin_key'], how='left', suffixes=('', '_txcount'))

            # calculate weighted mean by multiplying the value of each chain with the txcount of that chain and then summing up all values and dividing by the sum of all txcounts
            df_tmp['value'] = df_tmp['value'] * df_tmp['value_txcount']
            df_tmp = df_tmp.groupby(['date', 'unix', 'metric_key']).sum().reset_index()
            df_tmp['value'] = df_tmp['value'] / df_tmp['value_txcount']

            # drop value_txcount column
            df_tmp.drop(columns=['value_txcount'], inplace=True)

        df_tmp = df_tmp.loc[df_tmp.metric_key.isin(mks), ["unix", "value", "metric_key"]].pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp.columns.name = None

        df_tmp = self.df_rename(df_tmp, metric_id, True)

        mk_list = df_tmp.values.tolist()

        if len(self.metrics[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list]
        elif len(self.metrics[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list]
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        if rolling_avg == True:
            mk_list_int = self.create_7d_rolling_avg(mk_list_int)
        
        chains_list_no_eth = self.chains_list.copy()
        chains_list_no_eth.remove('ethereum')

        dict = {
            "metric_name": self.metrics[metric_id]['name'],
            "source": self.db_connector.get_metric_sources(metric_id, chains_list_no_eth),
            "avg": "true",
            "daily": {
                "types": df_tmp.columns.to_list(),
                "data": mk_list_int
            }
        }

        return dict
    
    def get_default_selection(self, df):
        df_tmp = df[df['metric_key'] == 'aa_last7d']

        ## filter out Ethereuem and only keep chains that are in the API and have a deployment of "PROD"
        df_tmp = df_tmp.loc[df_tmp.origin_key != 'ethereum']
        df_tmp = df_tmp.loc[df_tmp.origin_key.isin(self.chains_list_in_api_prod)]

        df_tmp = df_tmp.loc[df_tmp.date == df_tmp.date.max()]
        df_tmp = df_tmp.sort_values(by='value', ascending=False)
        top_5 = df_tmp['origin_key'].head(5).tolist()
        print(f'Default selection by aa_last7d: {top_5}')
        return top_5
    
    def gen_l2beat_link(self, chain):
        if chain.l2beat_tvl_naming:
            return f'https://l2beat.com/scaling/projects/{chain.l2beat_tvl_naming}'
        else:
            return 'https://l2beat.com'

    def gen_l2beat_stage(self, chain):
        stage = self.db_connector.get_stage(chain.origin_key)
        if stage == 'Stage 0':
            hex = "#FF8B36"
        elif stage == 'Stage 1':
            hex = "#FFEC44"
        elif stage == 'Stage 2':
            hex = "#34762F"
        else:
            hex = "#DFDFDF"      
        
        return {'stage': stage, 'hex': hex}
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{self.api_version}/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)

    ############################################################
    ##### Main Platfrom #####
    ############################################################
    def create_master_json(self, df_data):
        exec_string = "SELECT category_id, category_name, main_category_id, main_category_name FROM vw_oli_category_mapping"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## create dict with main_category_key as key and main_category_name as value, same for sub_categories
        main_category_dict = {}
        sub_category_dict = {}
        for index, row in df.iterrows():
            main_category_dict[row['main_category_id']] = row['main_category_name']
            sub_category_dict[row['category_id']] = row['category_name']


        ## create main_category <> sub_category mapping dict
        df_mapping = df.groupby(['main_category_id']).agg({'category_id': lambda x: list(x)}).reset_index()
        mapping_dict = {}
        for index, row in df_mapping.iterrows():
            mapping_dict[row['main_category_id']] = row['category_id']

        ## create dict with all chain info
        chain_dict = {}
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_api == False:
                print(f'..skipped: Master json export for {origin_key}. API is set to False')
                continue

            chain_dict[origin_key] = {
                'name': chain.name,
                'caip2': self.db_connector.get_chain_info(origin_key, 'caip2'),
                'evm_chain_id': self.db_connector.get_chain_info(origin_key, 'evm_chain_id'),
                'deployment': chain.deployment,
                'name_short': chain.name_short,
                'description': chain.description,
                'da_layer': chain.da_layer,
                'symbol': chain.symbol,
                'bucket': chain.bucket,
                'technology': chain.technology,
                'purpose': chain.purpose,
                'launch_date': chain.launch_date,
                'enable_contracts': chain.in_labels_api,
                'l2beat_stage': self.gen_l2beat_stage(chain),
                'l2beat_link': self.gen_l2beat_link(chain),
                'raas': chain.raas,
                'stack': chain.stack,
                'website': chain.website,
                'twitter': chain.twitter,
                'block_explorer': chain.block_explorer,
                'block_explorers': chain.block_explorers,
                'rhino_listed': bool(getattr(chain, 'rhino_naming', None)),
                'rhino_naming': getattr(chain, 'rhino_naming', None)
            }
        
        ## create dict for fees without metric_keys field
        fees_types_api = {key: {sub_key: value for sub_key, value in sub_dict.items() if sub_key != 'metric_keys'} 
                                  for key, sub_dict in self.fees_types.items()}

        master_dict = {
            'current_version' : self.api_version,
            'default_chain_selection' : self.get_default_selection(df_data),
            'chains' : chain_dict,
            'metrics' : self.metrics,
            'fee_metrics' : fees_types_api,
            'blockspace_categories' : {
                'main_categories' : main_category_dict,
                'sub_categories' : sub_category_dict,
                'mapping' : mapping_dict,
            }
        }

        master_dict = fix_dict_nan(master_dict, 'master')

        if self.s3_bucket == None:
            self.save_to_json(master_dict, 'master')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/master', master_dict, self.cf_distribution_id)

        print(f'DONE -- master.json export')

    def create_landingpage_json(self, df):
        # filter df for all_l2s (all chains except chains that aren't included in the API)
        chain_keys = self.chains_list_in_api + ['multiple']
        df = df.loc[(df.origin_key.isin(chain_keys))]

        landing_dict = {
            "data": {
                "metrics" : {
                    "user_base" : {
                        "metric_name": "Ethereum ecosystem user-base",
                        "source": self.db_connector.get_metric_sources('user_base', []),
                        "weekly": {
                            "latest_total": self.chain_users(df, 'weekly', 'all_l2s'),
                            "latest_total_comparison": self.create_chain_users_comparison_value(df, 'weekly', 'all_l2s'),
                            "l2_dominance": self.l2_user_share(df, 'weekly'),
                            "l2_dominance_comparison": self.create_l2_user_share_comparison_value(df, 'weekly'),
                            "cross_chain_users": self.cross_chain_users(df),
                            "cross_chain_users_comparison": self.create_cross_chain_users_comparison_value(df),
                            "chains": self.generate_chains_userbase_dict(df, 'weekly')
                            }
                        },
                    "table_visual" : self.get_landing_table_dict(df)
                    },
                "all_l2s": {
                    "chain_id": "all_l2s",
                    "chain_name": "All L2s",
                    "symbol": "-",
                    "metrics": {}
                },
                "top_contracts": {
                }
            }
        }

        for metric_id in ['txcount', 'stables_mcap', 'fees', 'rent_paid', 'market_cap']:
            landing_dict['data']['all_l2s']['metrics'][metric_id] = self.generate_all_l2s_metric_dict(df, metric_id, rolling_avg=True)

         ## put all origin_keys from adapter_mapping in a list where in_api is True
        chain_keys = [chain.origin_key for chain in adapter_mapping if chain.in_api == True and 'blockspace' not in chain.exclude_metrics]

        for days in [1,7,30,90,180,365]:
            contracts = self.db_connector.get_top_contracts_for_all_chains_with_change(top_by='gas', days=days, origin_keys=chain_keys, limit=6)

            # replace NaNs with Nones
            contracts = contracts.replace({np.nan: None})
            contracts = db_addresses_to_checksummed_addresses(contracts, ['address'])

            contracts = contracts[
                ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", 
                 "txcount", "daa", "gas_fees_eth_change", "gas_fees_usd_change", "txcount_change", "daa_change", "prev_gas_fees_eth", "prev_gas_fees_usd", 
                 "prev_txcount", "prev_daa", "gas_fees_eth_change_percent", "gas_fees_usd_change_percent", "txcount_change_percent", "daa_change_percent"]
            ]

            ## change column names contract_name to name and origin_key to chain
            contracts = contracts.rename(columns={'contract_name': 'name', 'origin_key': 'chain'})
            
            landing_dict['data']['top_contracts'][f"{days}d"] = {
                'types': contracts.columns.to_list(),
                'data': contracts.values.tolist()
            }

        landing_dict = fix_dict_nan(landing_dict, 'landing_page')

        if self.s3_bucket == None:
            self.save_to_json(landing_dict, 'landing_page')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/landing_page', landing_dict, self.cf_distribution_id)
        print(f'DONE -- landingpage export')

    def create_chain_details_jsons(self, df, origin_keys:list=None):
        if origin_keys != None:
            ## create adapter_mapping_filtered that only contains chains that are in the origin_keys list
            adapter_mapping_filtered = [chain for chain in adapter_mapping if chain.origin_key in origin_keys]
        else:
            adapter_mapping_filtered = adapter_mapping

        ## loop over all chains and generate a chain details json for all chains and with all possible metrics
        for chain in adapter_mapping_filtered:
            origin_key = chain.origin_key
            if chain.in_api == False:
                print(f'..skipped: Chain details export for {origin_key}. API is set to False')
                continue

            metrics_dict = {}
            ranking_dict = {}
            for metric in self.metrics:
                if self.metrics[metric]['fundamental'] == False:
                    continue

                if metric in chain.exclude_metrics:
                    print(f'..skipped: Chain details export for {origin_key} - {metric}. Metric is excluded')
                    continue
                
                mk_list = self.generate_daily_list(df, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                metrics_dict[metric] = {
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, [origin_key]),
                    'avg': self.metrics[metric]['avg'],
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    }
                }

                if self.metrics[metric]['ranking_bubble']:
                    ranking_dict[metric] = self.get_ranking(df, metric, origin_key)
            
            ## Hottest Contract
            if chain.aggregate_blockspace and 'blockspace' not in chain.exclude_metrics:
                hottest_contract = self.db_connector.get_top_contracts_for_all_chains_with_change(top_by='gas', days=1, origin_keys=[origin_key], limit=1)
                hottest_contract = hottest_contract.replace({np.nan: None})
                hottest_contract = db_addresses_to_checksummed_addresses(hottest_contract, ['address'])
                
                hottest_contract = hottest_contract[
                    ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key",
                     "gas_fees_eth", "gas_fees_usd", "gas_fees_eth_change", "gas_fees_usd_change", "prev_gas_fees_eth",
                     "prev_gas_fees_usd", "gas_fees_eth_change_percent", "gas_fees_usd_change_percent"]
                ]

                ## change column names contract_name to name and origin_key to chain
                hottest_contract = hottest_contract.rename(columns={'contract_name': 'name', 'origin_key': 'chain'})

                hottest_contract_dict = {
                    'types': hottest_contract.columns.to_list(),
                    'data': hottest_contract.values.tolist()
                }
            else:
                print(f'..skipped: Hottest Contract for {origin_key}. Flag aggregate_blockspace is set to False or blockspace is excluded.')
                hottest_contract_dict = None

            details_dict = {
                'data': {
                    'chain_id': origin_key,
                    'chain_name': chain.name,
                    'ranking': ranking_dict,
                    'hottest_contract': hottest_contract_dict,
                    'metrics': metrics_dict
                }
            }

            details_dict = fix_dict_nan(details_dict, f'chains/{origin_key}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'chains/{origin_key}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/chains/{origin_key}', details_dict, self.cf_distribution_id)
            print(f'DONE -- Chain details export for {origin_key}')

    def create_metric_details_jsons(self, df, metric_keys:list=None):
        if metric_keys != None:
            ## create metrics_filtered that only contains metrics that are in the metric_keys list
            metrics_filtered = {key: value for key, value in self.metrics.items() if key in metric_keys}
        else:
            metrics_filtered = self.metrics

        ## loop over all metrics and generate a metric details json for all metrics and with all possible chainn
        for metric in metrics_filtered:
            if self.metrics[metric]['fundamental'] == False:
                continue
            
            chains_dict = {}    
            for chain in adapter_mapping:
                origin_key = chain.origin_key
                if chain.in_api == False:
                    print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                    continue

                if metric in chain.exclude_metrics:
                    print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                    continue

                mk_list = self.generate_daily_list(df, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key)
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                chains_dict[origin_key] = {
                    'chain_name': chain.name,
                    'changes': self.create_changes_dict(df, metric, origin_key),
                    'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    },
                    'last_30d': self.value_last_30d(df, metric, origin_key),
                    'monthly': {
                        'types' : mk_list_columns_monthly,
                        'data' : mk_list_int_monthly
                    }
                }

                ## check if metric should be averagd and add 7d rolling avg field
                if self.metrics[metric]['avg'] == True:
                    mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
                    chains_dict[origin_key]['daily_7d_rolling'] = {
                        'types' : mk_list_columns,
                        'data' : mk_list_int_7d
                    }

            details_dict = {
                'data': {
                    'metric_id': metric,
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, []),
                    'avg': self.metrics[metric]['avg'],
                    'monthly_agg': 'distinct' if self.metrics[metric]['monthly_agg'] in ['maa'] else self.metrics[metric]['monthly_agg'],
                    'chains': chains_dict
                }
            }

            details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'metrics/{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/metrics/{metric}', details_dict, self.cf_distribution_id)
            print(f'DONE -- Metric details export for {metric}')

    #######################################################################
    ### FEES PAGE
    #######################################################################

    def create_fees_table_json(self, df):
        fees_dict = {
            "chain_data" : {}
        } 

        ## filter df timestamp to be within the last 48 hours (not more needed for table visual)
        df = df[(df.granularity == 'hourly')]
        df = df[df['timestamp'].dt.tz_localize(None) > datetime.now() - timedelta(hours=30)]
        max_ts_all = df['unix'].max()

        ## loop over all chains and generate a fees json for all chains
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_fees_api == False:
                print(f'..skipped: Fees export for {origin_key}. API is set to False')
                continue
            
            eth_price = self.db_connector.get_last_price_usd('ethereum')

            hourly_dict = {}
            for metric in self.fees_types:             
                generated = self.generate_fees_list(df, metric, origin_key, 'hourly', '7d', eth_price, max_ts_all, True)
                hourly_dict[metric] = {
                    "types": generated[1],
                    "data": generated[0]
                }
            
            fees_dict["chain_data"][origin_key] = {
                'hourly': hourly_dict,
                # 'ten_min': min_10_dict
            }

        fees_dict = fix_dict_nan(fees_dict, f'fees/table', False)

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, f'fees/table')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees/table', fees_dict, self.cf_distribution_id)
        print(f'DONE -- fees/table.json export')

    def create_fees_linechart_json(self, df):
        fees_dict = {
            "chain_data" : {}
        } 

        ## loop over all chains and generate a fees json for all chains
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_fees_api == False:
                print(f'..skipped: Fees export for {origin_key}. API is set to False')
                continue
            if origin_key == 'ethereum':
                print(f'..skipped: Fees export for {origin_key}. Ethereum is not included in the line chart')
                continue  
            
            eth_price = self.db_connector.get_last_price_usd('ethereum')                
            fees_dict["chain_data"][origin_key] = {}

            for metric in self.fees_types:
                timespan_dict = {}
                for timespan in self.fees_timespans:
                    granularity = self.fees_timespans[timespan]["granularity"]
                    filter_days = self.fees_timespans[timespan]["filter_days"]
                    df_tmp = df[df['timestamp'].dt.tz_localize(None) > datetime.now() - timedelta(hours=24*filter_days)].copy()
                    generated = self.generate_fees_list(df_tmp, metric, origin_key, granularity, timespan, eth_price)
                    timespan_dict[timespan] = {
                        "types": generated[1],
                        "granularity": granularity,
                        "data": generated[0]
                    }
            

                fees_dict["chain_data"][origin_key][metric] = timespan_dict

        fees_dict = fix_dict_nan(fees_dict, f'fees/linechart')

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, f'fees/linechart')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees/linechart', fees_dict, self.cf_distribution_id)
        print(f'DONE -- fees/linechart.json export')

    #######################################################################
    ### ECONOMICS SECTION
    #######################################################################

    def aggregate_metric(self, df, origin_key, metric_key, days):
        df = df[(df['origin_key'] == origin_key) & (df['metric_key'] == metric_key)]
        df = df[(df['date'] >= df['date'].max() - pd.DateOffset(days=days-1))]
        val = df['value'].sum()
        return val

    def gen_da_metric_dict(self, df, metric, origin_key):
        mk_list = self.generate_daily_list(df, metric, origin_key)
        mk_list_int = mk_list[0]
        mk_list_columns = mk_list[1]

        metric_dict = {}
        metric_dict['metric_name'] = self.metrics[metric]['name']
        metric_dict['daily'] = {
            'types' : mk_list_columns,
            'data' : mk_list_int
        }

        return metric_dict

    def create_economics_json(self, df):
        economics_dict = {
            "data": {
                "da_charts" : {
                    "ethereum": {
                        "total_blob_size": self.gen_da_metric_dict(df, 'total_blob_size', 'ethereum'),
                        "total_blob_fees": self.gen_da_metric_dict(df, 'total_blob_fees', 'ethereum'),
                    },
                    "celestia": {
                        "total_blob_size": self.gen_da_metric_dict(df, 'total_blob_size', 'celestia'),
                        "total_blob_fees": self.gen_da_metric_dict(df, 'total_blob_fees', 'celestia'),
                    },
                },
                "chain_breakdown": {}
            }
        }

        # filter df for all_l2s (all chains except chains that aren't included in the API)
        chain_keys = [x.origin_key for x in adapter_mapping if x.in_economics_api == True and x.deployment == "PROD"]
        df = df.loc[(df.origin_key.isin(chain_keys))]
        timeframes = [1,7,30,90,180,'max']
        
        # iterate over each chain and generate table and chart data
        for origin_key in chain_keys:
            economics_dict['data']['chain_breakdown'][origin_key] = {}
            # get data for each timeframe (for table aggregates)
            for timeframe in timeframes:
                timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'    
                days = timeframe if timeframe != 'max' else 2000

                revenue = self.aggregate_metric(df, origin_key, 'fees_paid_eth', days)
                profit = self.aggregate_metric(df, origin_key, 'profit_eth', days)
                profit_margin = profit / revenue if revenue != 0 else 0.0

                economics_dict['data']['chain_breakdown'][origin_key][timeframe_key] = {
                    "revenue": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'fees_paid_usd', days), self.aggregate_metric(df, origin_key, 'fees_paid_eth', days)]
                    },		
                    "costs": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'costs_total_usd', days), self.aggregate_metric(df, origin_key, 'costs_total_eth', days)],
                        "l1_costs": [self.aggregate_metric(df, origin_key, 'costs_l1_usd', days), self.aggregate_metric(df, origin_key, 'costs_l1_eth', days)], 
                        "blobs": [self.aggregate_metric(df, origin_key, 'costs_blobs_usd', days), self.aggregate_metric(df, origin_key, 'costs_blobs_eth', days)],
                    },
                    "profit": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'profit_usd', days), self.aggregate_metric(df, origin_key, 'profit_eth', days)]
                    },	
                    "profit_margin": {
                        "types": ["percentage"],
                        "total": [profit_margin]
                    },
                    "size": {
                        "types": ["bytes"],
                        "total": [self.aggregate_metric(df, origin_key, 'blob_size_bytes', days)]
                    }
                }

            ## add timeseries data for each chain
            economics_dict['data']['chain_breakdown'][origin_key]['daily'] = {}

            econ_metrics = ['fees', 'costs', 'profit']
            ## determine the min date for all metric_keys of econ_metrics in df
            min_date_fees = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['fees']['metric_keys']))]['date'].min()
            min_date_costs = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['costs']['metric_keys']))]['date'].min()
            min_date_profit = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['profit']['metric_keys']))]['date'].min()
            start_date = max(min_date_fees, min_date_costs, min_date_profit)

            for metric in econ_metrics:
                mk_list = self.generate_daily_list(df, metric, origin_key, start_date)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                metric_to_key = {
                    'fees': 'revenue',
                    'costs': 'costs',
                    'profit': 'profit'
                }

                key = metric_to_key.get(metric)

                economics_dict['data']['chain_breakdown'][origin_key]['daily'][key] = {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }

        economics_dict = fix_dict_nan(economics_dict, 'economics')

        if self.s3_bucket == None:
            self.save_to_json(economics_dict, 'economics')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/economics', economics_dict, self.cf_distribution_id)
        print(f'DONE -- economics export')

    #######################################################################
    ### LABELS
    #######################################################################

    def create_labels_json(self, type='full'):
        if type == 'full':
            limit = 250000
        elif type == 'quick':
            limit = 100
        else:
            raise ValueError('type must be either "full" or "quick"')
        
        order_by = 'txcount'
        df = self.db_connector.get_labels_lite_db(limit=limit, order_by=order_by, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['gas_fees_usd'] = df['gas_fees_usd'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['txcount_change'] = df['txcount_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)

        df['deployment_date'] = df['deployment_date'].apply(lambda x: str(x))
        df['deployment_date'] = df['deployment_date'].replace('NaT', None)

        df = df.replace({np.nan: None})        

        labels_dict = {
            'data': {
                'sort': {
                    'by': order_by,
                    'direction': 'desc'
                },
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        labels_dict = fix_dict_nan(labels_dict, f'labels-{type}')

        if self.s3_bucket == None:
            self.save_to_json(labels_dict, f'labels-{type}')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/{type}', labels_dict, self.cf_distribution_id)
        print(f'DONE -- labels {type} export')

    def create_labels_sparkline_json(self):
        df = self.db_connector.get_labels_page_sparkline(origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        df['txcount'] = df['txcount'].astype(int)
        df['daa'] = df['daa'].astype(int)
        df['gas_fees_usd'] = df['gas_fees_usd'].round(2)

        sparkline_dict = {
                'data': {'types': ['unix', 'txcount', 'gas_fees', 'active_addresses'],}
        }

        for address, origin_key in df[['address', 'origin_key']].drop_duplicates().values:
            sparkline_dict['data'][f'{origin_key}_{address}'] = {
                    'sparkline': df[(df['address'] == address) & (df['origin_key'] == origin_key)][['unix', 'txcount', 'gas_fees_usd', 'daa']].values.tolist()
            }                     

        sparkline_dict = fix_dict_nan(sparkline_dict, 'labels-sparkline')

        if self.s3_bucket == None:
            self.save_to_json(sparkline_dict, 'labels-sparkline')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/sparkline', sparkline_dict, self.cf_distribution_id)
        print(f'DONE -- sparkline export')

    def create_projects_json(self):        
        df = self.db_connector.get_active_projects()
        df = df.rename(columns={'name': 'owner_project'})
        df = df.replace({np.nan: None})        

        projects_dict = {
            'data': {
                'types': df.columns.to_list(),
                'data': df.values.tolist()
            }
        }

        projects_dict = fix_dict_nan(projects_dict, f'projects')

        if self.s3_bucket == None:
            self.save_to_json(projects_dict, f'prpjects')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/projects', projects_dict, self.cf_distribution_id)
        print(f'DONE -- projects export')

    def create_labels_parquet(self, type='full'):
        if type == 'full':
            limit = 250000
        elif type == 'quick':
            limit = 100
        else:
            raise ValueError('type must be either "full" or "quick"')
        
        order_by = 'txcount'
        df = self.db_connector.get_labels_lite_db(limit=limit, order_by=order_by, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['gas_fees_usd'] = df['gas_fees_usd'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['txcount_change'] = df['txcount_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/{type}', df, self.cf_distribution_id)
        print(f'DONE -- labels {type}.parquet export')

    def create_projects_parquet(self):        
        df = self.db_connector.get_active_projects()

        df = df.rename(columns={'name': 'owner_project'})

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/projects', df, self.cf_distribution_id)
        print(f'DONE -- labels projects.parquet export')

    def create_labels_sparkline_parquet(self):
        df = self.db_connector.get_labels_page_sparkline(limit = 1000000, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        df['txcount'] = df['txcount'].astype(int)
        df['daa'] = df['daa'].astype(int)
        df['gas_fees_usd'] = df['gas_fees_usd'].round(2)                 

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/sparkline', df, self.cf_distribution_id)
        print(f'DONE -- sparkline.parquet export')

    #######################################################################
    ### API ENDPOINTS
    #######################################################################

    def create_fundamentals_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## filter out all metric_keys that end with _eth
        df = df[~df.metric_key.str.endswith('_eth')]

        ## filter date to be in the last 365 days
        df = df.loc[df.date >= (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')]

        ## only keep metrics that are also in the metrics_list (based on metrics dict)
        df = df[df.metric_key.isin(self.metrics_list)]

        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in adapter_mapping
        for adapter in adapter_mapping:
            ## filter out origin_keys from df if in_api=false
            if adapter.in_api == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        ## put dataframe into a json string
        fundamentals_dict = df.to_dict(orient='records')

        fundamentals_dict = fix_dict_nan(fundamentals_dict, 'fundamentals')

        if self.s3_bucket == None:
            self.save_to_json(fundamentals_dict, 'fundamentals')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fundamentals', fundamentals_dict, self.cf_distribution_id)
    
    def create_fundamentals_full_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## only keep metrics that are also in the metrics_list (based on metrics dict)
        df = df[df.metric_key.isin(self.metrics_list)]

        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in adapter_mapping
        for adapter in adapter_mapping:
            ## filter out origin_keys from df if in_api=false
            if adapter.in_api == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        ## put dataframe into a json string
        fundamentals_dict = df.to_dict(orient='records')

        fundamentals_dict = fix_dict_nan(fundamentals_dict, 'fundamentals_full')

        if self.s3_bucket == None:
            self.save_to_json(fundamentals_dict, 'fundamentals_full')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fundamentals_full', fundamentals_dict, self.cf_distribution_id)

    ## TODO: DEPRECATE
    def create_contracts_json(self):
        exec_string = f"""
            SELECT address, contract_name, project_name, sub_category_key, origin_key
            FROM public.blockspace_labels;
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        contracts_dict = df.to_dict(orient='records')
        
        contracts_dict = fix_dict_nan(contracts_dict, 'contracts')

        if self.s3_bucket == None:
            self.save_to_json(contracts_dict, 'contracts')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/contracts', contracts_dict, self.cf_distribution_id)

    def create_glo_json(self):
        glo = Glo(self.db_connector)
        
        df = glo.run_glo()
        df_mcap = self.db_connector.get_glo_mcap()

        current_mcap = df_mcap[(df_mcap['metric_key'] == 'market_cap_usd') & (df_mcap['date'] == df_mcap['date'].max())].value.max()
        df['share'] = df['balance'] / current_mcap

        df_mcap['date'] = pd.to_datetime(df_mcap['date']).dt.tz_localize('UTC')
        df_mcap['unix'] = df_mcap['date'].apply(lambda x: x.timestamp() * 1000)
        df_mcap = df_mcap.drop(columns=['date'])
        df_mcap = df_mcap.pivot(index='unix', columns='metric_key', values='value')
        df_mcap.reset_index(inplace=True)
        df_mcap.rename(columns={'market_cap_eth':'eth', 'market_cap_usd':'usd'}, inplace=True)
        df_mcap = df_mcap.sort_values(by='unix', ascending=True)

        glo_dict = {'holders_table':{}, 'chart':{}, 'source':[]}

        for index, row in df.iterrows():
            glo_dict['holders_table'][row['holder']] = {'balance':row['balance'], 'share':row['share'], 'website':row['website'], 'twitter':row['twitter']}

        glo_dict['chart']['types'] = df_mcap.columns.to_list()
        glo_dict['chart']['data'] = df_mcap.values.tolist()
        glo_dict['source'] = ["Dune"]

        glo_dict = fix_dict_nan(glo_dict, 'GLO Dollar')

        if self.s3_bucket == None:
            self.save_to_json(glo_dict, 'GLO Dollar')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/glo_dollar', glo_dict, self.cf_distribution_id)

    def create_all_jsons(self):
        df = self.get_all_data()
        self.create_master_json(df)
        self.create_landingpage_json(df)

        self.create_chain_details_jsons(df)
        self.create_metric_details_jsons(df)

        self.create_economics_json(df)
        
        self.create_fundamentals_json(df)
        self.create_fundamentals_full_json(df)
        self.create_contracts_json()