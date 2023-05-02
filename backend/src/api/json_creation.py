import json
import datetime
import pandas as pd

from src.adapters.mapping import adapter_mapping, AdapterMapping
from src.misc.helper_functions import upload_json_to_cf_s3

class JSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector):
        ## Constants
        self.api_version = 'v0_4'
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector

        self.metrics = {
            'tvl': {
                'name': 'Total value locked (on chain)',
                'metric_keys': ['tvl', 'tvl_eth'],
                'units': ['USD', 'ETH']
            }
            ,'txcount': {
                'name': 'Transaction count',
                'metric_keys': ['txcount'],
                'units': ['-']
            }
            ,'daa': {
                'name': 'Daily active addresses',
                'metric_keys': ['daa'],
                'units': ['-']
            }
            ,'stables_mcap': {
                'name': 'Stablecoin market cap',
                'metric_keys': ['stables_mcap', 'stables_mcap_eth'],
                'units': ['USD', 'ETH']
            }
            ,'fees': {
                'name': 'Fees paid',
                'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
                'units': ['USD', 'ETH']
            }
            # ,'rent_paid': {
            #     'name': 'Rent paid',
            #     'metric_keys': ['rent_paid_usd', 'rent_paid_eth'],
            #     'units': ['USD', 'ETH'],
            #     'source' : 'Flipside'
            # }
        }

        #append all values of metric_keys in metrics dict to a list
        self.metrics_list = [item for sublist in [self.metrics[metric]['metric_keys'] for metric in self.metrics] for item in sublist]
        #concat all values of metrics_list to a string and add apostrophes around each value
        self.metrics_string = "'" + "','".join(self.metrics_list) + "'"

        #append all keys of chains dict to a list
        self.chains_list = [x.origin_key for x in adapter_mapping]
        #concat all values of chains_list to a string and add apostrophes around each value
        self.chains_string = "'" + "','".join(self.chains_list) + "'"

    
    ###### CHAIN DETAILS AND METRIC DETAILS METHODS ########

    def df_rename(self, df, metric_id, col_name_removal=False):
        # print(f'called df_rename for {metric_id}')
        # print(df.columns.to_list())
        if col_name_removal:
            df.columns.name = None

        if 'USD' in self.metrics[metric_id]['units'] or 'ETH' in self.metrics[metric_id]['units']:
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


    # this method returns a list of lists with the unix timestamp and all associated values for a certain metric_id and chain_id
    def generate_daily_list(self, df, metric_id, origin_key):
        ##print(f'called generate int for {metric_id} and {chain_id}')
        mks = self.metrics[metric_id]['metric_keys']
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["unix", "value", "metric_key"]].pivot(index='unix', columns='metric_key', values='value').reset_index()
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
                if df_tmp[mk].shape[0] <= (change):
                    change_val = 0
                else:
                    change_val = (cur_val - df_tmp[mk].iloc[change]) / df_tmp[mk].iloc[change]
                changes_dict[f'{change}d'].append(change_val)

        df_tmp = self.df_rename(df_tmp, metric_id)
        changes_dict['types'] = df_tmp.columns.to_list()

        return changes_dict

    def get_all_data(self):
        ## Load all data from database
        chain_user_list = self.chains_list + ['multiple']
        metric_user_list = self.metrics_list + ['user_base_daily', 'user_base_weekly', 'user_base_monthly', 'waa', 'maa']

        chain_user_string = "'" + "','".join(chain_user_list) + "'"
        metrics_user_string = "'" + "','".join(metric_user_list) + "'"

        df = self.download_data(chain_user_string, metrics_user_string)
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

        
        # max_date = datetime.date.today() - datetime.timedelta(days=1) ## get yesterday's date
        # if aggregation == 'daily':
        #     pass
        # elif aggregation == 'weekly':
        #     if max_date.weekday() != 6: ## check if max date is a sunday (full week). if not, remove latest week
        #         df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
        # elif aggregation == 'monthly': 
        #     if not self.end_of_month(max_date): ## check if max date is the last of the month (full month). if not, remove latest month
        #         df_tmp = df_tmp.loc[df_tmp.date != df_tmp.date.max()]
        # else:
        #     raise NotImplementedError("Only daily, weekly and monthly aggregations are supported")

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

    ## This method divdes the total l2 users by the total users of the Ethereum chain 
    def l2_user_share(self, df, aggregation, comparison=False):
        l2_user_share = self.chain_users(df, aggregation, 'all_l2s', comparison) / self.chain_users(df, aggregation, 'ethereum', comparison)

        return round(l2_user_share,4)

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

    def generate_user_base_dict(self, df, chain, aggregation):
        total_l2_users = self.chain_users(df, aggregation, 'all_l2s')
        if chain.origin_key in ['ethereum', 'all_l2s']:
            user_share = 0
        else:
            user_share = round(self.chain_users(df, aggregation, chain.origin_key) / total_l2_users, 4)
        dict = {
                    "chain_name": chain.name,
                    "technology": chain.technology,
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

    def generate_chains_dict(self, df, aggregation):       
        adapter_multi_mapping = adapter_mapping + [AdapterMapping(origin_key='multiple', name='Multiple L2s', technology='-')] + [AdapterMapping(origin_key='all_l2s', name='All L2s', technology='-')]

        chains_dict = {} 
        for chain in adapter_multi_mapping:
            chains_dict[chain.origin_key] = self.generate_user_base_dict(df, chain, aggregation)
        return chains_dict
    
    def create_total_comparison_value(self, df, aggregation, origin_key):
        val = (self.chain_users(df, aggregation, origin_key) - self.chain_users(df, aggregation, origin_key, comparison=True)) / self.chain_users(df, aggregation, origin_key, comparison=True)
        return round(val, 4)
    
    def create_user_share_comparison_value(self, df, aggregation):
        current = self.l2_user_share(df, aggregation)
        previous = self.l2_user_share(df, aggregation, comparison=True)
        val = (current - previous) / previous 
        return round(val,4)
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp)


    ##### JSON GENERATION METHODS #####
    
    def create_chain_details_jsons(self, df):
        ## loop over all chains and generate a chain details json for all chains and with all possible metrics
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            metrics_dict = {}
            for metric in self.metrics:
                if origin_key == 'ethereum' and metric == 'tvl':
                    continue
                if origin_key == 'imx' and metric == 'fees':
                    continue
                mk_list = self.generate_daily_list(df, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                metrics_dict[metric] = {
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, [origin_key]),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    }
                }

            details_dict = {
                'data': {
                    'chain_id': origin_key,
                    'chain_name': chain.name,
                    'symbol': chain.symbol,
                    'metrics': metrics_dict
                }
            }

            #self.save_to_json(details_dict, f'chains/{origin_key}')
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/chains/{origin_key}', details_dict, self.cf_distribution_id)
            print(f'-- DONE -- Chain details export for {origin_key}')

    def create_metric_details_jsons(self, df):
        ## loop over all metrics and generate a metric details json for all metrics and with all possible chains

        for metric in self.metrics:
            chains_dict = {}    
            for chain in adapter_mapping:
                origin_key = chain.origin_key
                if origin_key == 'ethereum' and metric == 'tvl':
                    continue
                mk_list = self.generate_daily_list(df, metric, origin_key)
                mk_list_int = mk_list[0]
                mk_list_columns = mk_list[1]

                chains_dict[origin_key] = {
                    'chain_name': chain.name,
                    'changes': self.create_changes_dict(df, metric, origin_key),
                    'daily': {
                        'types' : mk_list_columns,
                        'data' : mk_list_int
                    }
                }

            details_dict = {
                'data': {
                    'metric_id': metric,
                    'metric_name': self.metrics[metric]['name'],
                    'source': self.db_connector.get_metric_sources(metric, []),
                    'chains': chains_dict
                }
            }

            #self.save_to_json(details_dict, f'metrics/{metric}')
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/metrics/{metric}', details_dict, self.cf_distribution_id)
            print(f'-- DONE -- Metric details export for {metric}')

    def create_master_json(self):
        ## create a master dict
        chain_dict = {}
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            chain_dict[origin_key] = {
                'name': chain.name,
                'symbol': chain.symbol,
                'technology': chain.technology,
                'launch_date': chain.launch_date,
                'website': chain.website,
                'twitter': chain.twitter,
                'block_explorer': chain.block_explorer
            }

        master_dict = {
            'current_version' : self.api_version,
            'chains' : chain_dict,
            'metrics' : self.metrics
        }


        #self.save_to_json(master_dict, 'master')
        upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/master', master_dict, self.cf_distribution_id)

    def create_landingpage_json(self, df):
        landing_dict = {
            "data": {
                "metrics" : {
                    "user_base" : {
                        "metric_name": "Ethereum ecosystem user-base",
                        "source": self.db_connector.get_metric_sources('user_base', []),
                        "daily": {
                            "latest_total": self.chain_users(df, 'daily', 'all_l2s'),
                            "latest_total_comparison": self.create_total_comparison_value(df, 'daily', 'all_l2s'),
                            "l2_dominance": self.l2_user_share(df, 'daily'),
                            "l2_dominance_comparison": self.create_user_share_comparison_value(df, 'daily'),
                            "chains": self.generate_chains_dict(df, 'daily')
                            },
                        "weekly": {
                            "latest_total": self.chain_users(df, 'weekly', 'all_l2s'),
                            "latest_total_comparison": self.create_total_comparison_value(df, 'weekly', 'all_l2s'),
                            "l2_dominance": self.l2_user_share(df, 'weekly'),
                            "l2_dominance_comparison": self.create_user_share_comparison_value(df, 'weekly'),
                            "chains": self.generate_chains_dict(df, 'weekly')
                            },
                        "monthly": {
                            "latest_total": self.chain_users(df, 'monthly', 'all_l2s'),
                            "latest_total_comparison": self.create_total_comparison_value(df, 'monthly', 'all_l2s'),
                            "l2_dominance": self.l2_user_share(df, 'monthly'),
                            "l2_dominance_comparison": self.create_user_share_comparison_value(df, 'monthly'),
                            "chains": self.generate_chains_dict(df, 'monthly')		
                            }
                        }
                    }
                }
            }

        #self.save_to_json(landing_dict, 'landing_page')
        upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/landing_page', landing_dict, self.cf_distribution_id)
        print(f'-- DONE -- landingpage export')

    def create_all_jsons(self):
        df = self.get_all_data()
        self.create_chain_details_jsons(df)
        self.create_metric_details_jsons(df)
        self.create_master_json()
        self.create_landingpage_json(df)