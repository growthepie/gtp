import os
import simplejson as json
import datetime
import pandas as pd
import numpy as np
from datetime import timedelta, datetime, timezone
import getpass
sys_user = getpass.getuser()

from src.main_config import get_main_config, get_multi_config
from src.misc.helper_functions import upload_json_to_cf_s3, upload_parquet_to_cf_s3, db_addresses_to_checksummed_addresses, string_addresses_to_checksummed_addresses, fix_dict_nan
from src.misc.glo_prep import Glo
from src.db_connector import DbConnector
from eim.funcs import read_yaml_file

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

    def __init__(self, s3_bucket, cf_distribution_id, db_connector:DbConnector, api_version):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.main_config = get_main_config(self.db_connector)
        self.multi_config = get_multi_config(self.db_connector)
        self.latest_eth_price = self.db_connector.get_last_price_usd('ethereum')

        if sys_user == 'ubuntu':
            self.eth_exported_entities = read_yaml_file(f'/home/{sys_user}/gtp/backend/eim/eth_exported_entities.yml')
            self.ethereum_upgrades = json.load(open(f'/home/{sys_user}/gtp/backend/eim/ethereum_protocol_upgrades.json'))
        else:
            self.eth_exported_entities = read_yaml_file('eim/eth_exported_entities.yml')
            self.ethereum_upgrades = json.load(open('eim/ethereum_protocol_upgrades.json'))


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
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
            }
            ,'fees': {
                'name': 'Revenue',
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False
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
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False
            }

            ## Non Fundamental Metrics
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
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False
            }

            ,'costs_l1': {
                'name': 'L1 Costs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_l1_usd', 'costs_l1_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False
            }

            ,'costs_blobs': {
                'name': 'Blobs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_blobs_usd', 'costs_blobs_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False
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

        self.da_layers = {
            'da_ethereum_calldata': {
                'name': 'Ethereum Calldata',
                'name_short': 'Eth Calldata',
                'logo': {
                    'body': "<g clip-path=\"url(#clip0_14909_47267)\"><path d=\"M7.03607 9.8997C6.8081 10.1886 6.79639 10.6104 7.0566 10.8706C7.30666 11.1207 7.71403 11.1273 7.94101 10.8561C8.3029 10.4237 8.60019 9.93972 8.82253 9.41909C9.12835 8.70294 9.28602 7.9323 9.28602 7.15358C9.28602 6.37487 9.12836 5.60423 8.82254 4.88809C8.60021 4.36746 8.30291 3.88347 7.94103 3.45111C7.71404 3.17991 7.30668 3.18652 7.05661 3.43659C6.7964 3.69679 6.80812 4.11861 7.03608 4.40748C7.2721 4.70656 7.46917 5.03555 7.62177 5.38668C7.86409 5.94423 7.98913 6.54566 7.98913 7.15359C7.98913 7.76152 7.86408 8.36295 7.62176 8.9205C7.46916 9.27163 7.27209 9.60063 7.03607 9.8997Z\" fill=\"url(#paint0_linear_14909_47267)\"/><path d=\"M5.19163 5.30154C4.93592 5.55724 4.94763 5.96527 5.15042 6.26469C5.56032 6.86991 5.56032 7.43711 5.15041 8.04234C4.94763 8.34175 4.93592 8.74978 5.19162 9.00549C5.44733 9.26119 5.86774 9.26423 6.08782 8.97729C7.00106 7.78659 7.00106 6.52043 6.08783 5.32973C5.86775 5.04279 5.44733 5.04583 5.19163 5.30154Z\" fill=\"url(#paint1_linear_14909_47267)\"/><path d=\"M15 7.64842L10.3801 0L8.64846 2.86679C9.06627 3.36684 9.40961 3.92632 9.66657 4.52804C10.0211 5.35812 10.2038 6.25139 10.2038 7.154C10.2038 7.86818 10.0894 8.57651 9.86601 9.25232L10.5002 9.5L15 7.64842Z\" fill=\"url(#paint2_linear_14909_47267)\"/><path d=\"M8.12891 11.8442C8.3176 11.7572 8.49387 11.6259 8.64479 11.4456C8.87154 11.1747 9.07641 10.8863 9.25751 10.5831L10.3803 11.2441L15.0004 8.52434L10.3803 14.9996L8.12891 11.8442Z\" fill=\"url(#paint3_linear_14909_47267)\"/><path d=\"M5.15557 3.98202C5.15557 4.3765 4.8358 4.69629 4.44132 4.6963L2.8889 4.69633C2.67344 4.69634 2.46607 4.79141 2.34597 4.97027C1.48673 6.24986 1.48673 7.93504 2.34598 9.21464C2.4661 9.39352 2.67349 9.4886 2.88896 9.48859L4.44133 9.48855C4.83583 9.48854 5.15563 9.80834 5.15564 10.2028L5.15565 11.4692C5.15566 11.8637 4.83588 12.1835 4.4414 12.1835L2.67067 12.1836C2.5935 12.1836 2.5182 12.1588 2.4573 12.1114C2.25038 11.9504 2.05113 11.7747 1.86098 11.5846C-0.620281 9.10333 -0.62035 5.08037 1.86091 2.59911C2.05116 2.40886 2.25052 2.23323 2.45751 2.07214C2.51841 2.02475 2.59371 2.00001 2.67088 2.00001L4.44126 2C4.83575 2 5.15555 2.31979 5.15555 2.71428L5.15557 3.98202Z\" fill=\"url(#paint4_linear_14909_47267)\"/></g><defs><linearGradient id=\"paint0_linear_14909_47267\" x1=\"9.56995\" y1=\"5.07679\" x2=\"8.43344\" y2=\"11.8133\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#FE5468\"/><stop offset=\"1\" stop-color=\"#FFDF27\"/></linearGradient><linearGradient id=\"paint1_linear_14909_47267\" x1=\"9.56995\" y1=\"5.07679\" x2=\"8.43344\" y2=\"11.8133\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#FE5468\"/><stop offset=\"1\" stop-color=\"#FFDF27\"/></linearGradient><linearGradient id=\"paint2_linear_14909_47267\" x1=\"11.5646\" y1=\"0\" x2=\"11.5646\" y2=\"14.9996\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#10808C\"/><stop offset=\"1\" stop-color=\"currentColor\"/></linearGradient><linearGradient id=\"paint3_linear_14909_47267\" x1=\"11.5646\" y1=\"0\" x2=\"11.5646\" y2=\"14.9996\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#10808C\"/><stop offset=\"1\" stop-color=\"currentColor\"/></linearGradient><linearGradient id=\"paint4_linear_14909_47267\" x1=\"2.57783\" y1=\"2\" x2=\"2.57783\" y2=\"12.1836\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#10808C\"/><stop offset=\"1\" stop-color=\"currentColor\"/></linearGradient><clipPath id=\"clip0_14909_47267\"><rect width=\"15\" height=\"15\" fill=\"white\"/></clipPath></defs>",
                    'width': 15,
                    'height': 15
                },
                "colors": {
                    "light": [
                        "#00B3B3",
                        "#00B3B3"
                    ],
                    "dark": [
                        "#00B3B3",
                        "#00B3B3"
                    ],
                    "darkTextOnBackground": True
                },
            },
            'da_ethereum_blobs': {
                'name': 'Ethereum Blobs',
                'name_short': 'Eth Blobs',
                'logo': {
                    'body': "<g clip-path=\"url(#clip0_14914_47301)\"><path fill-rule=\"evenodd\" clip-rule=\"evenodd\" d=\"M12.8225 4C14.0251 4 15 4.97486 15 6.17742C15 7.37997 14.0251 8.35484 12.8225 8.35484C12.4902 8.35484 12.175 8.28031 11.8929 8.14699C11.7327 8.56147 11.4845 8.9322 11.1713 9.23624C11.2368 9.22934 11.3034 9.22581 11.3709 9.22581C12.4131 9.22581 13.258 10.0707 13.258 11.1129C13.258 12.1551 12.4131 13 11.3709 13C10.3286 13 9.4837 12.1551 9.4837 11.1129C9.4837 10.6918 9.62161 10.303 9.85473 9.98903C9.59793 10.0593 9.32759 10.0968 9.04848 10.0968C7.36485 10.0968 6 8.73196 6 7.04839C6 5.36481 7.36485 4 9.04848 4C9.85007 4 10.5796 4.30953 11.1236 4.81528C11.5225 4.31844 12.1353 4 12.8225 4ZM14.129 6.17742C14.129 5.45589 13.5441 4.87097 12.8225 4.87097C12.101 4.87097 11.516 5.45589 11.516 6.17742C11.516 6.89895 12.101 7.48387 12.8225 7.48387C13.5441 7.48387 14.129 6.89895 14.129 6.17742ZM10.0646 6.17742C10.0646 5.8422 10.1247 5.52014 10.2348 5.22206C9.8935 4.99976 9.48649 4.87097 9.04848 4.87097C7.84589 4.87097 6.87099 5.84583 6.87099 7.04839C6.87099 8.25094 7.84589 9.22581 9.04848 9.22581C9.84017 9.22581 10.5343 8.8031 10.9156 8.16974C10.3919 7.6683 10.0646 6.96124 10.0646 6.17742ZM12.387 11.1129C12.387 10.5517 11.9321 10.0968 11.3709 10.0968C10.8096 10.0968 10.3547 10.5517 10.3547 11.1129C10.3547 11.6741 10.8096 12.129 11.3709 12.129C11.9321 12.129 12.387 11.6741 12.387 11.1129Z\" fill=\"url(#paint0_linear_14914_47301)\"/><path d=\"M-4.86374e-05 7.64842L4.61991 0L6.35154 2.86679C5.93373 3.36684 5.59039 3.92632 5.33343 4.52804C4.97895 5.35812 4.7962 6.25139 4.7962 7.154C4.7962 7.86818 4.91062 8.57651 5.13399 9.25232L4.49978 9.5L-4.86374e-05 7.64842Z\" fill=\"url(#paint1_linear_14914_47301)\"/><path d=\"M6.87109 11.8442C6.6824 11.7572 6.50613 11.6259 6.35521 11.4456C6.12846 11.1747 5.92359 10.8863 5.74249 10.5831L4.61975 11.2441L-0.000378609 8.52434L4.61975 14.9996L6.87109 11.8442Z\" fill=\"url(#paint2_linear_14914_47301)\"/></g><defs><linearGradient id=\"paint0_linear_14914_47301\" x1=\"10.5\" y1=\"4\" x2=\"4.43296\" y2=\"12.5296\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#FE5468\"/><stop offset=\"1\" stop-color=\"#FFDF27\"/></linearGradient><linearGradient id=\"paint1_linear_14914_47301\" x1=\"3.43536\" y1=\"0\" x2=\"3.43536\" y2=\"14.9996\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#10808C\"/><stop offset=\"1\" stop-color=\"currentColor\"/></linearGradient><linearGradient id=\"paint2_linear_14914_47301\" x1=\"3.43536\" y1=\"0\" x2=\"3.43536\" y2=\"14.9996\" gradientUnits=\"userSpaceOnUse\"><stop stop-color=\"#10808C\"/><stop offset=\"1\" stop-color=\"currentColor\"/></linearGradient><clipPath id=\"clip0_14914_47301\"><rect width=\"15\" height=\"15\" fill=\"white\"/></clipPath></defs>",
                    'width': 15,
                    'height': 15
                },
                "colors": {
                    "light": [
                        "#FFC300",
                        "#FFC300"
                    ],
                    "dark": [
                        "#FFC300",
                        "#FFC300"
                    ],
                    "darkTextOnBackground": True
                },
            },
            'da_celestia': {
                'name': 'Celestia',
                'name_short': 'Celestia',
                'logo': {
                    'body': "<g clip-path=\"url(#clip0_10823_38559)\"> <path fill-rule=\"evenodd\" clip-rule=\"evenodd\" d=\"M14.3761 7.70448C14.3747 7.75095 14.3114 7.76323 14.2931 7.7204C14.2853 7.70191 14.2773 7.68337 14.2691 7.66482C14.2542 7.63089 14.2584 7.59143 14.2806 7.56175C14.2852 7.55568 14.2946 7.5434 14.3058 7.52877C14.3359 7.48944 14.3792 7.49531 14.3792 7.53895C14.3792 7.59622 14.3779 7.6458 14.3761 7.70448ZM14.0431 6.79524C13.9919 6.88758 13.8604 6.89183 13.8035 6.80279C13.7946 6.78903 13.7857 6.77527 13.7768 6.76144C13.5716 6.44638 13.3354 6.13711 13.0717 5.83655C13.007 5.76276 13.0599 5.64695 13.1581 5.64688H13.1605C13.7768 5.64688 14.0999 5.79912 14.1985 5.98501C14.2808 6.14049 14.2555 6.41238 14.0431 6.79524ZM13.9054 10.0182C13.8723 10.1023 13.8377 10.1859 13.8014 10.2686C13.7957 10.28 13.7904 10.2916 13.7845 10.3029C13.5577 10.7395 13.1771 11.0833 12.6684 11.3226C12.5543 11.3762 12.4294 11.2718 12.4634 11.1504C12.6323 10.5459 12.7601 9.8903 12.843 9.20304C12.8584 9.07488 12.9216 8.95725 13.0193 8.8728C13.2285 8.69183 13.4238 8.51113 13.6035 8.33171L13.6054 8.32968C13.6939 8.24146 13.8447 8.27943 13.8807 8.39909C14.0582 8.98922 14.0681 9.53888 13.9054 10.0182ZM12.5228 12.2006C12.4711 12.2559 12.4182 12.3106 12.3644 12.3644C12.2707 12.458 12.1747 12.5486 12.0766 12.6361C12.0147 12.6912 11.9212 12.6223 11.9562 12.5471C11.9662 12.5257 11.9761 12.5041 11.9859 12.4823C12.0272 12.3915 12.067 12.2982 12.1057 12.2033C12.1148 12.1809 12.1341 12.1641 12.1576 12.1581C12.2551 12.1334 12.3502 12.1061 12.4429 12.0762C12.5179 12.052 12.5767 12.143 12.5228 12.2006ZM11.5074 11.655C11.3021 11.6847 11.0862 11.7039 10.8603 11.7119C10.3092 11.7312 9.72639 11.6836 9.12777 11.5734C9.05478 11.56 9.04075 11.4613 9.10692 11.4279C9.23751 11.3617 9.36816 11.2942 9.49875 11.225C10.4147 10.7394 11.2694 10.2087 12.0176 9.66461C12.0737 9.6238 12.151 9.67196 12.1396 9.74043C12.0359 10.364 11.8922 10.951 11.7118 11.4839C11.681 11.5747 11.6023 11.6412 11.5074 11.655ZM11.2957 12.4858C10.8046 13.4517 10.1998 13.994 9.61348 13.983C8.97627 13.9717 8.34088 13.3081 7.87034 12.1626L7.8698 12.1613C7.83992 12.0886 7.87425 12.0054 7.94629 11.9741C7.95128 11.9718 7.95627 11.9697 7.96127 11.9675C7.99216 11.954 8.02683 11.9516 8.05934 11.9608C8.93951 12.2078 9.80673 12.3367 10.6192 12.3367C10.7075 12.3367 10.7952 12.3352 10.8822 12.3322C10.984 12.3285 11.0841 12.3228 11.1825 12.315C11.2738 12.3078 11.3373 12.4041 11.2957 12.4858ZM8.38749 14.3226C8.09549 14.3601 7.79918 14.3793 7.49997 14.3793C6.16281 14.3793 4.88387 14.0004 3.78583 13.2933C3.71029 13.2446 3.74239 13.1274 3.83217 13.1232C3.85079 13.1224 3.86819 13.1214 3.8835 13.1205C4.16255 13.1023 4.46851 13.0615 4.80003 12.9976C5.50523 12.8619 6.28915 12.6292 7.10996 12.3158C7.18355 12.2878 7.26571 12.3243 7.29566 12.3971L7.2962 12.3984C7.6143 13.1729 8.00578 13.7581 8.44536 14.1304C8.51801 14.1919 8.48192 14.3104 8.38749 14.3226ZM4.40929 9.50252C5.01547 9.00892 5.72573 8.51639 6.50486 8.0501C6.52159 9.04662 6.63086 10.0033 6.82424 10.8675C6.53528 10.7443 6.24719 10.6086 5.96161 10.4603C5.40844 10.1728 4.88799 9.85077 4.40929 9.50252ZM2.52409 12.1726C2.37948 11.8998 2.56638 11.2678 3.51589 10.3072L3.51637 10.3067C3.61552 10.2066 3.60601 10.0422 3.49532 9.95499C3.45728 9.92511 3.41951 9.89489 3.382 9.86467C3.28744 9.78825 3.15058 9.79526 3.06471 9.88126C3.04589 9.90008 3.03186 9.91425 3.02775 9.9185C2.5531 10.4057 2.2209 10.8567 2.0348 11.2623C1.98205 11.3773 1.82273 11.3897 1.75299 11.2842C1.01635 10.1706 0.620686 8.86578 0.620686 7.49997C0.620686 7.01169 0.671409 6.53116 0.77009 6.0644C0.784997 5.99378 0.884622 5.98926 0.905532 6.05833C1.04826 6.52955 1.28056 7.01695 1.60156 7.50981C2.03365 8.1734 2.60348 8.81129 3.27874 9.39642C3.50922 9.59615 3.75265 9.78933 4.00653 9.97569C4.52119 10.3534 5.08042 10.7019 5.67541 11.011C5.93915 11.1481 6.196 11.2789 6.45906 11.3973C6.6055 11.4632 6.77217 11.4683 6.92164 11.4096C7.04568 11.3611 7.1706 11.3106 7.29626 11.258C7.43292 11.201 7.50705 11.052 7.47083 10.9084C7.46631 10.8905 7.46273 10.876 7.46071 10.8675C7.25357 9.9958 7.13843 9.01445 7.12487 7.98656L7.12446 7.98676C7.12433 7.97455 7.1244 7.96214 7.12426 7.9498C7.12366 7.89361 7.12399 7.83702 7.12298 7.78097C7.12264 7.76309 7.12318 7.73281 7.12339 7.71487C7.12352 7.70772 7.12339 7.70063 7.12339 7.69355C7.12372 7.6292 7.12447 7.56485 7.12561 7.5003C7.12878 7.32399 7.13512 7.14895 7.14423 6.97533C7.2081 5.73497 7.41808 4.56968 7.75271 3.58928C7.78367 3.49856 7.7353 3.40008 7.64458 3.36912C7.55615 3.3389 7.46779 3.31003 7.37957 3.28238C7.29033 3.25439 7.19495 3.30255 7.1648 3.39111C6.80212 4.45583 6.57831 5.71898 6.51862 7.05566C6.511 7.22524 6.41872 7.37957 6.27269 7.46611C5.48102 7.9353 4.75396 8.43268 4.12585 8.93405C3.995 9.03853 3.80809 9.03455 3.68155 8.9248C3.05203 8.37899 2.5222 7.78616 2.12168 7.17107C1.33459 5.96248 1.14755 4.82391 1.59515 3.96566C1.59637 3.96363 1.59745 3.96154 1.59859 3.95959C1.65566 3.86489 1.71501 3.77147 1.7768 3.67933C2.29111 2.99854 3.24838 2.60402 4.51795 2.55944C4.59828 2.5566 4.67916 2.55519 4.76077 2.55519C5.5395 2.55519 6.37522 2.68368 7.22517 2.92893C7.24379 2.93433 7.2626 2.93973 7.28122 2.94519C7.29559 2.94944 7.30996 2.95342 7.32432 2.95773C7.52074 3.01642 7.7177 3.08151 7.91493 3.15247C7.91951 3.15409 7.92417 3.15577 7.92875 3.15739C7.98015 3.17594 8.03148 3.19496 8.08281 3.21432C8.09887 3.22039 8.11499 3.22653 8.13104 3.23273C8.14649 3.23867 8.16193 3.2444 8.17745 3.25041C8.59254 3.41107 9.0071 3.59818 9.41666 3.81106C10.2094 4.22292 10.9351 4.70573 11.5665 5.23543C10.3614 5.5115 8.96055 6.0441 7.55211 6.76219C7.48715 6.79537 7.44439 6.86066 7.44027 6.93351C7.43535 7.02079 7.42887 7.1963 7.42422 7.33579C7.42132 7.42334 7.51393 7.48135 7.5915 7.44061L7.59231 7.44014C8.98679 6.70789 10.3785 6.16059 11.5737 5.87136C11.7971 5.81733 11.9417 5.60277 11.9103 5.37512C11.8883 5.21607 11.8636 5.05951 11.8362 4.90573C11.8018 4.71254 11.696 4.5398 11.54 4.42095C10.9802 3.99453 10.3633 3.60338 9.70285 3.26019C9.25781 3.02896 8.80623 2.82634 8.3537 2.65353C8.2569 2.61657 8.21542 2.50217 8.26594 2.41172C8.73216 1.57768 9.28668 1.11058 9.8269 1.11058C9.83249 1.11058 9.83809 1.11058 9.84369 1.11072C10.4809 1.12212 11.1163 1.78564 11.5868 2.93116C11.825 3.51104 12.0096 4.17975 12.1357 4.90647C12.135 4.90586 12.1343 4.90532 12.1337 4.90471C12.1876 5.21485 12.2309 5.53538 12.2631 5.86421C12.2603 5.86596 12.2574 5.86751 12.2546 5.86927C12.2584 5.87311 12.2624 5.87696 12.2662 5.88087C12.3196 6.43262 12.3421 7.00717 12.3316 7.59345C12.325 7.9643 12.3052 8.33002 12.2732 8.68785C11.4207 9.3768 10.3666 10.0624 9.20803 10.6764C8.82727 10.8783 8.44658 11.0661 8.06959 11.2391C8.06372 11.2418 8.05792 11.2444 8.05205 11.2471C8.02325 11.2602 7.99438 11.2736 7.96558 11.2866L7.96491 11.2864C7.65254 11.4277 7.34314 11.5584 7.03887 11.6778C7.02167 11.6846 7.00474 11.6913 6.98761 11.698C6.96622 11.7064 6.94484 11.7146 6.92353 11.7228C6.87247 11.7424 6.82161 11.7617 6.77089 11.7806C6.75692 11.7858 6.74289 11.7913 6.72893 11.7964C6.66735 11.8193 6.6061 11.8415 6.54506 11.8633C6.53744 11.866 6.52982 11.8689 6.52226 11.8716C5.87021 12.1038 5.24878 12.2792 4.68273 12.3881C3.31489 12.6514 2.6708 12.4494 2.52409 12.1726ZM3.50652 1.89706C4.66425 1.06802 6.04767 0.620686 7.49997 0.620686C7.88329 0.620686 8.26169 0.652051 8.63247 0.713229C8.73176 0.72962 8.76508 0.85636 8.68589 0.918483C8.32658 1.20016 7.99276 1.61694 7.69733 2.15743C7.62522 2.28929 7.47231 2.35378 7.3277 2.31304C6.34891 2.0375 5.38591 1.90786 4.49616 1.93909C4.16794 1.95062 3.85713 1.98387 3.56513 2.0377C3.48311 2.05281 3.43873 1.94556 3.50652 1.89706ZM12.9495 6.91071C12.9475 6.83186 13.0487 6.7982 13.0944 6.86242C13.1506 6.94127 13.2048 7.02052 13.2567 7.10018C13.3256 7.20608 13.3897 7.31137 13.4494 7.41599C13.4865 7.48121 13.4776 7.56303 13.427 7.61848C13.3467 7.7067 13.2596 7.7979 13.1649 7.89246C13.1304 7.92693 13.0952 7.96147 13.0593 7.99607C13.0148 8.0391 12.9401 8.00632 12.9425 7.9444C12.9469 7.83156 12.9502 7.71831 12.9522 7.60452C12.9564 7.3714 12.9554 7.13998 12.9495 6.91071ZM12.2361 2.5106C12.2794 2.55161 12.3221 2.5933 12.3644 2.63559C13.0273 3.2985 13.5379 4.07311 13.8794 4.91672C13.9138 5.00178 13.8416 5.09081 13.7513 5.07456C13.5537 5.03908 13.3314 5.02323 13.0853 5.02734C12.9147 5.03017 12.7664 4.91065 12.7365 4.7427C12.6027 3.99271 12.4093 3.29991 12.161 2.69528C12.1468 2.66081 12.1325 2.62675 12.118 2.59303C12.0864 2.51944 12.178 2.45543 12.2361 2.5106ZM14.838 5.94009C14.5416 4.53191 13.8447 3.23806 12.8032 2.19662C11.3867 0.780073 9.50333 0 7.49997 0C5.49667 0 3.61323 0.780073 2.19668 2.19662C1.85214 2.54116 1.54571 2.91342 1.27854 3.30814C1.19908 3.41391 1.12792 3.52561 1.06465 3.6425C0.370644 4.79571 0 6.11937 0 7.49997C0 9.50333 0.78014 11.3867 2.19668 12.8032C3.61323 14.2198 5.49667 15 7.49997 15C9.50333 15 11.3867 14.2198 12.8032 12.8032C13.4718 12.1347 13.9984 11.3622 14.3683 10.5221C14.4135 10.429 14.453 10.3337 14.4869 10.2365C14.8236 9.37612 15 8.45163 15 7.49997C15 6.97614 14.9462 6.46074 14.8421 5.95952C14.8407 5.95304 14.8394 5.94656 14.838 5.94009Z\" fill=\"url(#paint0_linear_10823_38559)\"/> </g> <defs> <linearGradient id=\"paint0_linear_10823_38559\" x1=\"7.5\" y1=\"0\" x2=\"7.5\" y2=\"15\" gradientUnits=\"userSpaceOnUse\"> <stop stop-color=\"#10808C\"/> <stop offset=\"1\" stop-color=\"currentColor\"/> </linearGradient> <clipPath id=\"clip0_10823_38559\"> <rect width=\"15\" height=\"15\" fill=\"white\"/> </clipPath> </defs>",
                    'width': 15,
                    'height': 15
                },
                "colors": {
                    "light": [
                        "#8E44ED",
                        "#8E44ED"
                    ],
                    "dark": [
                        "#8E44ED",
                        "#8E44ED"
                    ],
                    "darkTextOnBackground": True
                },
            },
        }

        self.da_metrics = {
            'blob_count': {
                'name': 'Blob Count',
                'fundamental': True,
                'metric_keys': ['da_blob_count'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}
                },
                'avg': True, ##7d rolling average
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': False
            }
            ,'data_posted': {
                'name': 'Data Posted',
                'fundamental': True,
                'metric_keys': ['da_data_posted_bytes'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True, 'suffix': 'GB'}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': False
            }
            ,'fees_paid': {
                'name': 'Fees Paid',
                'fundamental': True,
                'metric_keys': ['da_fees_usd', 'da_fees_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': True
            }
            ,'fees_per_mbyte': {
                'name': 'Fees Paid per MB',
                'fundamental': True,
                'metric_keys': ['da_fees_per_mbyte_usd', 'da_fees_per_mbyte_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 6, 'decimals_tooltip': 6, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'avg',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': True
            }
            ,'blob_producers': {
                'name': 'Blob Producers',
                'fundamental': True,
                'metric_keys': ['da_unique_blob_producers'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': False
            }
        }

        self.eim_metrics = {
            'eth_exported': {
                'name': 'ETH exported',
                'fundamental': True,
                'metric_keys': ['eth_equivalent_exported_usd', 'eth_equivalent_exported_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': False, ##7d rolling average
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': False
            },
            'eth_supply': {
                'name': 'ETH supply',
                'fundamental': True,
                'metric_keys': ['eth_supply_eth'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': False, ##7d rolling average
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': False
            },
            'eth_issuance_rate': {
                'name': 'ETH issuance rate',
                'fundamental': True,
                'metric_keys': ['eth_issuance_rate'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': False, ##7d rolling average
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': False
            }
        }

        for metric_key, metric_value in self.metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.fees_types.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.da_metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        for metric_key, metric_value in self.eim_metrics.items():
            metric_value['units'] = {key: merge_dicts(self.units.get(key, {}), value) for key, value in metric_value['units'].items()}

        #append all values of metric_keys in metrics dict to a list
        self.metrics_list = [item for sublist in [self.metrics[metric]['metric_keys'] for metric in self.metrics] for item in sublist]
        self.da_metrics_list = [item for sublist in [self.da_metrics[metric]['metric_keys'] for metric in self.da_metrics] for item in sublist]
        #concat all values of metrics_list to a string and add apostrophes around each value
        self.metrics_string = "'" + "','".join(self.metrics_list) + "'"

        #append all keys of chains dict to a list
        self.chains_list = [x.origin_key for x in self.main_config]
        #concat all values of chains_list to a string and add apostrophes around each value
        self.chains_string = "'" + "','".join(self.chains_list) + "'"
        #only chains that are in the api output
        self.chains_list_in_api = [chain.origin_key for chain in self.main_config if chain.api_in_main == True]
        #only chains that are in the api output and deployment is "PROD"
        self.chains_list_in_api_prod = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and chain.api_deployment_flag=='PROD']
        #only chains that are in the api output and deployment is "PROD" and in_labels_api is True
        self.chains_list_in_api_labels = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and chain.api_in_labels == True]

        self.chains_list_in_api_economics = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True]

        self.da_layers_list = [layer for layer in self.da_layers]

        ## all feest metrics keys
        self.fees_list = [item for sublist in [self.fees_types[metric]['metric_keys'] for metric in self.fees_types] for item in sublist]

    
    ###### CHAIN DETAILS AND METRIC DETAILS METHODS ########

    def get_metric_dict(self, metric_type):
        if metric_type == 'default':
            return self.metrics
        elif metric_type == 'da':
            return self.da_metrics
        elif metric_type == 'eim':
            return self.eim_metrics
        else:
            raise ValueError(f"ERROR: metric type {metric_type} is not implemented")

    def df_rename(self, df, metric_id, tmp_metrics_dict, col_name_removal=False):
        if col_name_removal:
            df.columns.name = None

        if 'usd' in tmp_metrics_dict[metric_id]['units'] or 'eth' in tmp_metrics_dict[metric_id]['units']:
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
    
    def get_ranking(self, df, metric_id, origin_key, incl_value = False):
        mks = self.metrics[metric_id]['metric_keys']
        ## remove elements in mks list that end with _eth
        mks = [x for x in mks if not x.endswith('_eth')]

        ## First filter down to metric
        df_tmp = df.loc[(df.metric_key.isin(mks)), ["origin_key", "value", "metric_key", "date"]].copy()

        ## then max date of this metric per origin_key
        df_tmp = df_tmp.loc[df_tmp.groupby("origin_key")["date"].idxmax()]

        ## filter out chains that have this metric excluded
        chains_list_metric = [chain.origin_key for chain in self.main_config if metric_id not in chain.api_exclude_metrics]
        df_tmp = df_tmp.loc[(df_tmp.origin_key.isin(chains_list_metric))]

        ## filter out ethereum
        df_tmp = df_tmp.loc[(df_tmp.origin_key != 'ethereum')]

        ## assign rank based on order (descending order if metric_key is not 'txcosts')
        if metric_id != 'txcosts':
            df_tmp['rank'] = df_tmp['value'].rank(ascending=False, method='first')
        else:
            df_tmp['rank'] = df_tmp['value'].rank(ascending=True, method='first')

        ## get rank for origin_key
        if df_tmp.loc[df_tmp.origin_key == origin_key].shape[0] > 0:
            rank = df_tmp.loc[df_tmp.origin_key == origin_key, 'rank'].values[0]
            rank_max = df_tmp['rank'].max()

            if incl_value:
                 ## check if metric units include eth and usd
                if 'usd' in self.metrics[metric_id]['units'].keys():
                    is_currency = True
                else:
                    is_currency = False
                
                if is_currency:
                    value_usd = df_tmp.loc[df_tmp.origin_key == origin_key, 'value'].values[0]
                    value_eth = value_usd / self.latest_eth_price
                    return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2), 'value_usd': value_usd, 'value_eth': value_eth}
                else:
                    value = df_tmp.loc[df_tmp.origin_key == origin_key, 'value'].values[0]
                    return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2), 'value': value}
            else:
                #print(f"...rank for {origin_key} and {metric_id} is {int(rank)} out of {int(rank_max)}")
                return {'rank': int(rank), 'out_of': int(rank_max), 'color_scale': round(rank/rank_max, 2)}    
        else:
            #print(f"...no rank for {origin_key} and {metric_id}")
            return {'rank': None, 'out_of': None, 'color_scale': None}         


    # this method returns a list of lists with the unix timestamp and all associated values for a certain metric_id and chain_id
    def generate_daily_list(self, df, metric_id, origin_key, start_date = None, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)            

        mks = tmp_metrics_dict[metric_id]['metric_keys']
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["unix", "value", "metric_key", "date"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]
        
        max_date = df_tmp['date'].max()
        max_date = pd.to_datetime(max_date).replace(tzinfo=None)
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.date()

        ## if max_date_fill is True, fill missing rows until yesterday with 0
        if tmp_metrics_dict[metric_id]['max_date_fill']:
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
        
        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(tmp_metrics_dict[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(tmp_metrics_dict[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        return mk_list_int, df_tmp.columns.to_list()
    
    # this method returns a list of lists with the unix timestamp (first day of month) and all associated values for a certain metric_id and chain_id
    def generate_monthly_list(self, df, metric_id, origin_key, start_date = None, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'maa'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "unix", "value", "metric_key"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]

        ## create monthly averages on value and min on unix column
        df_tmp['date'] = df_tmp['date'].dt.tz_convert(None) ## get rid of timezone in order to avoid warnings

        ## replace earliest date with first day of month (in unix) in unix column
        df_tmp['unix'] = (df_tmp['date'].dt.to_period("M").dt.start_time).astype(np.int64) // 10**6        

        if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'sum', 'unix': 'min'}).reset_index()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
            df_tmp = df_tmp.groupby([df_tmp.date.dt.to_period("M"), df_tmp.metric_key]).agg({'value': 'mean', 'unix': 'min'}).reset_index()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
            pass ## no aggregation necessary
        else:
            raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")

        ## drop column date
        df_tmp = df_tmp.drop(columns=['date'])
        ## metric_key to column
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(tmp_metrics_dict[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(tmp_metrics_dict[metric_id]['units']) == 2:
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
                and kpi."date" >= '2022-01-01'
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
    
    def download_data_eim(self):
        exec_string = f"""
            SELECT 
                kpi.metric_key, 
                kpi.origin_key as origin_key, 
                kpi."date", 
                kpi.value
            FROM public.fact_eim kpi
            where kpi."date" >= '2015-01-01'
                and metric_key in ('eth_equivalent_exported_usd', 'eth_equivalent_exported_eth', 'eth_supply_eth', 'eth_issuance_rate')
        """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        ## datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        # fill NaN values with 0
        df.value.fillna(0, inplace=True)
        return df

    def create_changes_dict(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        #print(f'called create_changes_dict for {metric_id} and {origin_key}')
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(tmp_metrics_dict[metric_id]['metric_keys'])), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
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

        for mk in tmp_metrics_dict[metric_id]['metric_keys']:
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

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    def create_changes_dict_monthly(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)   

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
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
            if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
                cur_val = df_tmp[mk].iloc[0:29].sum()
            elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
                cur_val = df_tmp[mk].iloc[0:29].mean()
            elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
                cur_val = df_tmp[mk].iloc[0]
            else:
                raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")
            
            changes = [30,90,180,365]
            for change in changes:
                if df_tmp[mk].shape[0] <= change:
                    change_val = None
                else:
                    if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
                        prev_val = df_tmp[mk].iloc[change:change+29].sum()
                    elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
                        prev_val = df_tmp[mk].iloc[change:change+29].mean()
                    elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
                        prev_val = df_tmp[mk].iloc[change]
                    else:
                        raise NotImplementedError(f"monthly_agg {tmp_metrics_dict[metric_id]['monthly_agg']} not implemented")
                    
                    if (prev_val < 0) or (cur_val < 0) or (prev_val == 0):
                        change_val = None
                    else:
                        change_val = (cur_val - prev_val) / prev_val
                        change_val = round(change_val, 4)
                        if change_val >= 100:
                            change_val = 99.99
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict
    
    ## this function takes a dataframe and a metric_id and origin_key as input and returns a value that aggregates the last 30 days
    def value_last_30d(self, df, metric_id, origin_key, metric_type='default'):
        tmp_metrics_dict = self.get_metric_dict(metric_type)

        mks = tmp_metrics_dict[metric_id]['metric_keys'].copy()
        if 'daa' in mks:
            mks[mks.index('daa')] = 'aa_last30d'

        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["date", "value", "metric_key"]].pivot(index='date', columns='metric_key', values='value')
        df_tmp.sort_values(by=['date'], inplace=True, ascending=False)

        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        if tmp_metrics_dict[metric_id]['monthly_agg'] == 'sum':
            val = df_tmp.iloc[0:29].sum()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'avg':
            val = df_tmp.iloc[0:29].mean()
        elif tmp_metrics_dict[metric_id]['monthly_agg'] == 'maa':
            val = df_tmp.iloc[0]

        val_dict = {
            'types': val.keys().to_list(),
            'data': val.to_list()
        }

        return val_dict

    def get_all_data(self):
        ## Load all data from database
        chain_user_list = self.chains_list_in_api + self.da_layers_list + ['multiple', 'celestia']
        metric_user_list = self.metrics_list + self.da_metrics_list + ['user_base_daily', 'user_base_weekly', 'user_base_monthly', 'waa', 'maa', 'aa_last30d', 'aa_last7d', 
                                                'cca_last7d_exclusive', 'blob_size_bytes'] ## add metrics that are not in the metrics_list

        chain_user_string = "'" + "','".join(chain_user_list) + "'"
        metrics_user_string = "'" + "','".join(metric_user_list) + "'"

        df = self.download_data(chain_user_string, metrics_user_string)

        ## divide value by 1000000 where metric_key is gas_per_second --> mgas/s
        df.loc[df['metric_key'] == 'gas_per_second', 'value'] = df['value'] / 1000000
        df.loc[df['metric_key'] == 'da_data_posted_bytes', 'value'] = df['value'] / 1024 / 1024 / 1024
        return df
    
    def get_data_fees(self):
        df = self.download_data_fees(self.fees_list)
        return df
    
    def get_data_eim(self):
        df = self.download_data_eim()

        ## create new df that sums up all values for each metric_key/date combination and assign "total" to origin_key
        df_total = df.groupby(['date', 'metric_key']).sum().reset_index()
        df_total['origin_key'] = 'total'

        ## append df_total to df
        df = pd.concat([df, df_total], ignore_index=True)

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
        if chain.runs_aggregate_addresses == False or chain.origin_key == 'starknet':
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
        for chain in self.main_config:
            if chain.api_in_main == True and chain.origin_key != 'ethereum':
                ranking_dict = {}
                for metric in self.metrics:
                    if self.metrics[metric]['ranking_landing']:
                        ranking_dict[metric] = self.get_ranking(df, metric, chain.origin_key, incl_value=True)

                chains_dict[chain.origin_key] = {
                    "chain_name": chain.name,
                    "technology": chain.metadata_technology,
                    "purpose": chain.metadata_purpose,
                    "users": self.get_aa_last7d(df, chain.origin_key),
                    "user_share": round(self.get_aa_last7d(df, chain.origin_key)/all_users,4),
                    "cross_chain_activity": self.get_cross_chain_activity(df, chain),
                    "ranking": ranking_dict
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
                    "technology": chain.metadata_technology,
                    "purpose": chain.metadata_purpose,
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

        for chain in self.multi_config:
            chains_dict[chain.origin_key] = self.generate_userbase_dict(df, chain, aggregation)
        return chains_dict
    
    ## This method generates a dict containing aggregate daily values for all_l2s (all chains except Ethereum) for a specific metric_id
    def generate_all_l2s_metric_dict(self, df, metric_id, rolling_avg=False, economics_api=False, days=730):
        metric = self.metrics[metric_id]
        mks = metric['metric_keys']

        # filter df for all_l2s (all chains except Ethereum and chains that aren't included in the API)
        if economics_api == True:
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(self.chains_list_in_api_economics))]
        else:
            df_tmp = df.loc[(df.origin_key!='ethereum') & (df.metric_key.isin(mks)) & (df.origin_key.isin(self.chains_list_in_api))]

        # filter df _tmp by date so that date is greather than 2 years ago
        df_tmp = df_tmp.loc[df_tmp.date >= (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')]  
        
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

        df_tmp = self.df_rename(df_tmp, metric_id, self.metrics, col_name_removal=True)

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
        if chain.aliases_l2beat:
            return f'https://l2beat.com/scaling/projects/{chain.aliases_l2beat_slug}'
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
        ##main_category_dict = {}
        sub_category_dict = {}
        for index, row in df.iterrows():
            ##main_category_dict[row['main_category_id']] = row['main_category_name']
            sub_category_dict[row['category_id']] = row['category_name']

        main_category_dict = {
            'defi': 'DeFi',
            'cefi': 'CeFi',            
            'nft': 'NFT',
            'social': 'Social',
            'utility': 'Utility',
            'token_transfers': 'Token Transfers',
            'cross_chain': 'Cross-Chain',
            'unlabeled': 'Unlabeled'
        }

        ## create main_category <> sub_category mapping dict
        df_mapping = df.groupby(['main_category_id']).agg({'category_id': lambda x: list(x)}).reset_index()
        mapping_dict = {}
        for index, row in df_mapping.iterrows():
            mapping_dict[row['main_category_id']] = row['category_id']

        ## create dict with all chain info
        chain_dict = {}
        for chain in self.multi_config:
            origin_key = chain.origin_key
            if chain.api_in_main == False:
                print(f'..skipped: Master json export for {origin_key}. API is set to False')
                continue

            url_key = chain.origin_key.replace('_', '-')
            if chain.origin_key == 'imx':
                url_key = 'immutable-x'
            if chain.origin_key == 'rhino':
                url_key = 'rhino-fi'

            chain_dict[origin_key] = {
                'name': chain.name,
                'url_key': url_key,
                'chain_type': chain.chain_type,
                'caip2': self.db_connector.get_chain_info(origin_key, 'caip2'),
                'evm_chain_id': chain.evm_chain_id,
                'deployment': chain.api_deployment_flag,
                'name_short': chain.name_short,
                'description': chain.metadata_description,
                'da_layer': chain.metadata_da_layer,
                'symbol': chain.metadata_symbol,
                'bucket': chain.bucket,
                'ecosystem': chain.ecosystem,
                'colors': chain.colors,
                'logo': chain.logo,
                'technology': chain.metadata_technology,
                'purpose': chain.metadata_purpose,
                'launch_date': chain.metadata_launch_date,
                'enable_contracts': chain.api_in_labels,
                'l2beat_stage': self.gen_l2beat_stage(chain),
                'l2beat_link': self.gen_l2beat_link(chain),
                'l2beat_id': chain.aliases_l2beat,
                'raas': chain.metadata_raas,
                'stack': chain.metadata_stack,
                'website': chain.socials_website,
                'twitter': chain.socials_twitter,
                'block_explorer': next(iter(chain.block_explorers.values())) if chain.block_explorers else None,
                'block_explorers': chain.block_explorers,
                'rhino_listed': bool(getattr(chain, 'aliases_rhino', None)),
                'rhino_naming': getattr(chain, 'aliases_rhino', None)
            }

        ## sort chain_dict by key (origin_key) alphabetically asc
        chain_dict = dict(sorted(chain_dict.items()))
        ## move ethereum to the top
        chain_dict = dict(sorted(chain_dict.items(), key=lambda x: x[0] != 'ethereum'))

        da_dict = self.da_layers
        
        ## create dict for fees without metric_keys field
        fees_types_api = {key: {sub_key: value for sub_key, value in sub_dict.items() if sub_key != 'metric_keys'} 
                                  for key, sub_dict in self.fees_types.items()}

        master_dict = {
            'current_version' : self.api_version,
            'default_chain_selection' : self.get_default_selection(df_data),
            'chains' : chain_dict,
            'da_layers' : da_dict,
            'metrics' : self.metrics,
            'da_metrics' : self.da_metrics,
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

         ## put all origin_keys from main_config in a list where in_api is True
        chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_main == True and 'blockspace' not in chain.api_exclude_metrics]

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
            ## create main_conf_filtered that only contains chains that are in the origin_keys list
            main_config_filtered = [chain for chain in self.main_config if chain.origin_key in origin_keys]
        else:
            main_config_filtered = self.main_config

        ## loop over all chains and generate a chain details json for all chains and with all possible metrics
        for chain in main_config_filtered:
            origin_key = chain.origin_key
            if chain.api_in_main == False:
                print(f'..skipped: Chain details export for {origin_key}. API is set to False')
                continue

            metrics_dict = {}
            ranking_dict = {}
            for metric in self.metrics:
                if self.metrics[metric]['fundamental'] == False:
                    continue

                if metric in chain.api_exclude_metrics:
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
            if chain.runs_aggregate_blockspace and 'blockspace' not in chain.api_exclude_metrics:
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
            for chain in self.main_config:
                origin_key = chain.origin_key
                if chain.api_in_main == False:
                    print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                    continue

                if metric in chain.api_exclude_metrics:
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

    def create_da_metric_details_jsons(self, df, metric_keys:list=None):
        if metric_keys != None:
            ## create metrics_filtered that only contains metrics that are in the metric_keys list
            metrics_filtered = {key: value for key, value in self.da_metrics.items() if key in metric_keys}
        else:
            metrics_filtered = self.da_metrics

        ## loop over all metrics and generate a metric details json for all metrics and with all possible chainn
        for metric in metrics_filtered:
            if self.da_metrics[metric]['fundamental'] == False:
                continue
            
            da_dict = {}    
            for da in self.da_layers:
                origin_key = da
                # if chain.api_in_main == False:
                #     print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                #     continue

                # if metric in chain.api_exclude_metrics:
                #     print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                #     continue

                if metric in ['blob_count', 'blob_producers'] and origin_key == 'da_ethereum_calldata':
                    print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded')
                    continue

                mk_list = self.generate_daily_list(df, metric, origin_key, metric_type='da')
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key, metric_type='da')
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                da_dict[origin_key] = {
                    'changes': self.create_changes_dict(df, metric, origin_key, metric_type='da'),
                    'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key, metric_type='da'),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    },
                    'last_30d': self.value_last_30d(df, metric, origin_key, metric_type='da'),
                    'monthly': {
                        'types' : mk_list_columns_monthly,
                        'data' : mk_list_int_monthly
                    }
                }

                ## check if metric should be averagd and add 7d rolling avg field
                if self.da_metrics[metric]['avg'] == True:
                    mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
                    da_dict[origin_key]['daily_7d_rolling'] = {
                        'types' : mk_list_columns,
                        'data' : mk_list_int_7d
                    }

            details_dict = {
                'data': {
                    'metric_id': metric,
                    'metric_name': self.da_metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, []),
                    'avg': self.da_metrics[metric]['avg'],
                    'monthly_agg': 'distinct' if self.da_metrics[metric]['monthly_agg'] in ['maa'] else self.da_metrics[metric]['monthly_agg'],
                    'chains': da_dict
                }
            }

            details_dict = fix_dict_nan(details_dict, f'da_metrics/{metric}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'da_metrics/{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/da_metrics/{metric}', details_dict, self.cf_distribution_id)
            print(f'DONE -- DA Metric details export for {metric}')

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
        for chain in self.main_config:
            origin_key = chain.origin_key
            if chain.api_in_fees == False:
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
        for chain in self.main_config:
            origin_key = chain.origin_key
            if chain.api_in_fees == False:
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
        ##start_date is today 180 days ago
        start_date = datetime.now() - timedelta(days=130)
        start_date = start_date.replace(tzinfo=timezone.utc)  

        mk_list = self.generate_daily_list(df, metric, origin_key, start_date)
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
                "all_l2s" : {
                    "chain_id": "all_l2s",
                    "chain_name": "All L2s",
                    "metrics": {}
                },
                "chain_breakdown": {}
            }
        }

        economics_dict['data']['all_l2s']['metrics']['fees'] = self.generate_all_l2s_metric_dict(df, 'fees', rolling_avg=False, economics_api=True, days=365)

        economics_dict['data']['all_l2s']['metrics']['costs'] = {
            'costs_l1': self.generate_all_l2s_metric_dict(df, 'costs_l1', rolling_avg=False, economics_api=True, days=365),
            'costs_blobs': self.generate_all_l2s_metric_dict(df, 'costs_blobs', rolling_avg=False, economics_api=True, days=365)
            
        }

        economics_dict['data']['all_l2s']['metrics']['profit'] = self.generate_all_l2s_metric_dict(df, 'profit', rolling_avg=False, economics_api=True, days=365)

        # filter df for all_l2s (all chains except chains that aren't included in the API)
        # chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True and chain.api_deployment_flag == 'PROD']
        chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_economics == True]
        df = df.loc[(df.origin_key.isin(chain_keys))]
        timeframes = [1,7,30,90,180,365,'max']
        
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

                if profit_margin > 1:
                    profit_margin = 1

                economics_dict['data']['chain_breakdown'][origin_key][timeframe_key] = {
                    "revenue": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'fees_paid_usd', days), self.aggregate_metric(df, origin_key, 'fees_paid_eth', days)]
                    },		
                    "costs": {
                        "types": ["usd", "eth"],
                        "total": [self.aggregate_metric(df, origin_key, 'costs_total_usd', days), self.aggregate_metric(df, origin_key, 'costs_total_eth', days)],
                        "costs_l1": [self.aggregate_metric(df, origin_key, 'costs_l1_usd', days), self.aggregate_metric(df, origin_key, 'costs_l1_eth', days)], 
                        "costs_blobs": [self.aggregate_metric(df, origin_key, 'costs_blobs_usd', days), self.aggregate_metric(df, origin_key, 'costs_blobs_eth', days)],
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

            econ_metrics = ['fees', 'costs', 'profit']
            ## determine the min date for all metric_keys of econ_metrics in df
            min_date_fees = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['fees']['metric_keys']))]['date'].min()
            min_date_costs = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['costs']['metric_keys']))]['date'].min()
            min_date_profit = df.loc[(df.origin_key == origin_key) & (df.metric_key.isin(self.metrics['profit']['metric_keys']))]['date'].min()
            start_date = max(min_date_fees, min_date_costs, min_date_profit)

            metric_to_key = {
                    'fees': 'revenue',
                    'costs': 'costs',
                    'profit': 'profit'
                }
            
            ## add timeseries data for each chain
            economics_dict['data']['chain_breakdown'][origin_key]['daily'] = {}
            economics_dict['data']['chain_breakdown'][origin_key]['monthly'] = {}

            for metric in econ_metrics:
                key = metric_to_key.get(metric)

                ## Fill daily values
                mk_list = self.generate_daily_list(df, metric, origin_key, start_date)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]                

                economics_dict['data']['chain_breakdown'][origin_key]['daily'][key] = {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }

                ## Fill monthly values
                mk_list_monthly = self.generate_monthly_list(df, metric, origin_key, start_date)
                mk_list_int_monthly = mk_list_monthly[0]
                mk_list_columns_monthly = mk_list_monthly[1]

                economics_dict['data']['chain_breakdown'][origin_key]['monthly'][key] = {
                    'types' : mk_list_columns_monthly,
                    'data' : mk_list_int_monthly
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
        df = string_addresses_to_checksummed_addresses(df, ['deployer_address'])
        df['deployment_tx'] = df['deployment_tx'].str.lower()
        
        df['gas_fees_usd'] = df['gas_fees_usd'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['txcount_change'] = df['txcount_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: round(x, 4) if pd.notnull(x) else x)

        ## if txcount change > 99 then set to 99
        df['txcount_change'] = df['txcount_change'].apply(lambda x: 99.99 if x > 99.99 else x)
        df['gas_fees_usd_change'] = df['gas_fees_usd_change'].apply(lambda x: 99.99 if x > 99.99 else x)
        df['daa_change'] = df['daa_change'].apply(lambda x: 99.99 if x > 99.99 else x)

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
        df = self.db_connector.get_labels_page_sparkline(limit=10000, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        ## Fill in missing dates for each address, origin_key combination
        # Find min and max dates for each address, origin_key combination
        min_dates = df.groupby(['address', 'origin_key'])['date'].min().reset_index()
        max_dates = df.groupby(['address', 'origin_key'])['date'].max().reset_index()
        # Merge min and max dates
        date_ranges = pd.merge(min_dates, max_dates, on=['address', 'origin_key'])
        date_ranges.columns = ['address', 'origin_key', 'min_date', 'max_date']
        # Create a DataFrame for all combinations of address, origin_key, and date
        all_combinations = []
        for _, row in date_ranges.iterrows():
            address = row['address']
            origin_key = row['origin_key']
            all_dates = pd.date_range(start=row['min_date'], end=row['max_date'])
            all_combinations.append(pd.DataFrame({
                'date': all_dates,
                'address': address,
                'origin_key': origin_key
            }))
        # Concatenate all combinations into a single DataFrame
        all_combinations_df = pd.concat(all_combinations, ignore_index=True)
        # Merge with the original dataframe
        merged_df = pd.merge(all_combinations_df, df, on=['date', 'address', 'origin_key'], how='left')
        # Fill NaN values with 0
        merged_df['txcount'].fillna(0, inplace=True)
        merged_df['gas_fees_usd'].fillna(0, inplace=True)
        merged_df['daa'].fillna(0, inplace=True)

        df = merged_df.copy()

        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        df['txcount'] = df['txcount'].astype(int)
        df['daa'] = df['daa'].astype(int)
        df['gas_fees_usd'] = df['gas_fees_usd'].round(2)

        sparkline_dict = {
                'data': {'types': ['unix', 'txcount', 'gas_fees_usd', 'daa'],}
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

    def create_export_labels_json(self, limit, origin_key=None):
        if origin_key == None:
            origin_keys = self.chains_list_in_api_labels
        else:
            origin_keys = [origin_key]

        df = self.db_connector.get_labels_export_df(limit=limit, origin_keys=origin_keys, incl_aggregation=False)
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df = string_addresses_to_checksummed_addresses(df, ['deployer_address'])
        df['deployment_tx'] = df['deployment_tx'].str.lower()
        
        df = df.where(pd.notnull(df), None)
        df['deployment_date'] = df['deployment_date'].astype(str)        
        df['deployment_date'] = df['deployment_date'].replace('NaT', None)
        df = df.drop(columns=['origin_key']) ## chain_id is sufficient

        labels_dict = df.to_dict(orient='records')  

        if self.s3_bucket == None:
            self.save_to_json(labels_dict, f'export_labels')
        else:
            if origin_key == None:
                full_name = f'{self.api_version}/labels/export_labels'
            else:
                full_name = f'{self.api_version}/labels/export_labels_{origin_key}'

            upload_json_to_cf_s3(self.s3_bucket, full_name, labels_dict, self.cf_distribution_id)
        print(f'DONE -- labels {full_name} export')

    def create_export_labels_parquet(self, subset = 'top50k'):
        if subset == 'top50k':
            limit = 50000
        else:
            raise ValueError('subset must be either "top50k"')
        
        df = self.db_connector.get_labels_export_df(limit=limit, origin_keys=self.chains_list_in_api_labels)
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df = df.drop(columns=['origin_key']) ## chain_id is sufficient

        upload_parquet_to_cf_s3(self.s3_bucket, f'{self.api_version}/labels/export_labels_{subset}', df, self.cf_distribution_id)
        print(f'DONE -- labels export_labels_{subset}.parquet export')


    #######################################################################
    ### EIM
    #######################################################################

    def create_eth_exported_json(self, df):
        entity_dict = {}    
        metric = 'eth_exported'

        ## add 'total' to the list of entities
        self.eth_exported_entities['total'] = {
            'name': 'Total ETH Exported',
            'type': 'total',
            'chains': ['all']
        }

        for entity in self.eth_exported_entities:
            mk_list = self.generate_daily_list(df, metric, entity, metric_type='eim', start_date='2021-01-01')
            mk_list_int = mk_list[0]
            mk_list_columns = mk_list[1]

            entity_dict[entity] = {
                'changes': self.create_changes_dict(df, metric, entity, metric_type='eim'),
                'daily': {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }
            }        

        details_dict = {
            'data': {
                'metric_id': metric,
                'metric_name': self.eim_metrics[metric]['name'],
                'entities': self.eth_exported_entities,
                'chart': entity_dict
            }
        }

        details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

        if self.s3_bucket == None:
            self.save_to_json(details_dict, f'eim/eth_exported')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eim/eth_exported', details_dict, self.cf_distribution_id)
        print(f'DONE -- ETH exported export done')

    def create_eth_supply_json(self, df):
        metric_dict = {}
        metrics_list = ['eth_supply', 'eth_issuance_rate']
        ## keep only metrics from self.da_metrics that are in the metrics_list
        metrics = {key: value for key, value in self.eim_metrics.items() if key in metrics_list} 

        for metric in metrics:
            mk_list = self.generate_daily_list(df, metric, "ethereum", metric_type='eim')
            mk_list_int = mk_list[0]
            mk_list_columns = mk_list[1]

            metric_dict[metric] = {
                'daily': {
                    'types' : mk_list_columns,
                    'data' : mk_list_int
                }
            }        

        details_dict = {
            'data': {
                'chart': metric_dict,
                'events': self.ethereum_upgrades
            }
        }

        details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

        if self.s3_bucket == None:
            self.save_to_json(details_dict, f'eim/eth_supply')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eim/eth_supply', details_dict, self.cf_distribution_id)
        print(f'DONE -- ETH supply export done')


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
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
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
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
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

    def create_metrics_export_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()
        ## transform date column to string with format YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        ## filter based on settings in main_config
        for adapter in self.main_config:
            ## filter out origin_keys from df if in_api=false
            if adapter.api_in_main == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]
            elif len(adapter.api_exclude_metrics) > 0:
                origin_key = adapter.origin_key
                for metric in adapter.api_exclude_metrics:
                    if metric != 'blockspace':
                        #print(f"Filtering out metric_keys {metric} for adapter {adapter.name}")
                        metric_keys = self.metrics[metric]['metric_keys']
                        df = df[~((df.origin_key == origin_key) & (df.metric_key.isin(metric_keys)))]
        
        for metric in self.metrics:
            if self.metrics[metric]['fundamental'] == False:
                continue
            #print(f'...exporting metric {metric}')
            mks = self.metrics[metric]['metric_keys']
            df_metric = df[df['metric_key'].isin(mks)].copy()

            ## put dataframe into a json string
            metric_dict = df_metric.to_dict(orient='records')
            metric_dict = fix_dict_nan(metric_dict, f'metric_dict_{metric}')

            if self.s3_bucket == None:
                self.save_to_json(metric_dict, f'metric_{metric}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/export/{metric}', metric_dict, self.cf_distribution_id)

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