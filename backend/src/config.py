# Units
## Decimals: only relevant if value isn't aggregated
## When aggregated (starting >1k), we always show 2 decimals
## in case of ETH and decimals >6, show Gwei
## prefix and suffix should also show in axis
gtp_units = {
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

gtp_metrics = {
        'tvl': {
            'name': 'Total Value Secured',
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

gtp_da_metrics = {
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
            'name': 'DA Fees Paid',
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
            'name': 'DA Consumers',
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

gtp_app_metrics =  {
        'txcount': {
            'name': 'Transaction Count',
            'metric_keys': ['txcount'],
            'units': {
                'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
            },
            'avg': True,
            'all_l2s_aggregate': 'sum',
            'monthly_agg': 'sum',
            'max_date_fill' : False,
            'source': ['RPC', 'OLI']
        }
        ,'daa': {
            'name': 'Active Addresses',
            'metric_keys': ['daa'],
            'units': {
                'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
            },
            'avg': True,
            'all_l2s_aggregate': 'sum',
            'monthly_agg': 'maa',
            'max_date_fill' : False,
            'source': ['RPC', 'OLI']
        }
        ,'gas_fees': {
            'name': 'Fees Paid',
            'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
            'units': {
                'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
            },
            'avg': True,
            'all_l2s_aggregate': 'sum',
            'monthly_agg': 'sum',
            'max_date_fill' : False,
            'source': ['RPC', 'OLI']
        }
    }

gtp_fees_types = {
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

# Fees timespans
## mapping of timeframes to granularity, filter_days and tps_divisor
gtp_fees_timespans = {
        '24hrs' : {'granularity': '10_min', 'filter_days': 1, 'tps_divisor': 60*10},
        '7d' : {'granularity': 'hourly', 'filter_days': 7, 'tps_divisor': 60*60},
        '30d' : {'granularity': '4_hours', 'filter_days': 30, 'tps_divisor': 60*60*4},
        '180d' : {'granularity': 'daily', 'filter_days': 180, 'tps_divisor': 60*60*24},
    }

eth_maturity_levels = {
        "4_robust": {
            "name" : "Robust",
            "description" : "Fully decentralized and secure network that cannot be tampered with or stopped by any individual or group, including its creators. This is a network that fulfills Ethereum's vision of decentralization.",
            "conditions": {
                "and" : {
                    "tvl": 1000000000,
                    "stage": "Stage 2"
                }
            }
        },
        "3_maturing": {
            "name" : "Maturing",
            "description" : "A network transitioning to being decentralized. A group of actors still may be able to halt the network in extreme situations.",
            "conditions": {
                "and" : {
                    "tvl": 150000000,
                    "stage": "Stage 1",
                    "age": 180 
                }
            }
        },
        "2_developing": {
            "name" : "Developing",
            "description" : "A centralized operator runs the network but adds fail-safe features to reduce risks of centralization.",
            "conditions": {
                "and" : {
                    "tvl": 150000000,
                    "stage": "Stage 0",
                    "risks": 3,
                    "age": 180 
                }
            }
        },
        "1_emerging": {
            "name" : "Emerging",
            "description" : "A centralized operator runs the network. The data is publicly visible on Ethereum to verify whether the operator is being honest.",
            "conditions": {
                "and" : {
                    "stage": "Stage 0",
                    "risks": 2
                },
                "or" : {
                    "tvl": 150000000,
                    "age": 180 
                }
            }
        }
    }

eim_metrics = {
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