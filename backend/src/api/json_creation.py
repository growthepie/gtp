import os
import simplejson as json
import datetime
import pandas as pd
import numpy as np

from src.chain_config import adapter_mapping, adapter_multi_mapping
from src.misc.helper_functions import upload_json_to_cf_s3, db_addresses_to_checksummed_addresses, fix_dict_nan

class JSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector, api_version):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector

        self.metrics = {
            'tvl': {
                'name': 'Total value locked (on chain)',
                'metric_keys': ['tvl', 'tvl_eth'],
                'units': ['USD', 'ETH'],
                'avg': False,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg'
            }
            ,'txcount': {
                'name': 'Transaction count',
                'metric_keys': ['txcount'],
                'units': ['-'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum'
            }
            ,'daa': {
                'name': 'Daily active addresses',
                'metric_keys': ['daa'],
                'units': ['-'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'maa'
            }
            ,'stables_mcap': {
                'name': 'Stablecoin market cap',
                'metric_keys': ['stables_mcap', 'stables_mcap_eth'],
                'units': ['USD', 'ETH'],
                'avg': False,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg'
            }
            ,'fees': {
                'name': 'Fees paid',
                'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum'
            }
            ,'rent_paid': {
                'name': 'Rent paid to L1',
                'metric_keys': ['rent_paid_usd', 'rent_paid_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum'
            }
            ,'profit': {
                'name': 'Profit',
                'metric_keys': ['profit_usd', 'profit_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum'
            }
            ,'txcosts': {
                'name': 'Transaction costs',
                'metric_keys': ['txcosts_median_usd', 'txcosts_median_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'weighted_mean',
                'monthly_agg': 'avg'
            }
            ,'fdv': {
                'name': 'Fully diluted valuation',
                'metric_keys': ['fdv_usd', 'fdv_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg'
            }
            ,'market_cap': {
                'name': 'Market cap',
                'metric_keys': ['market_cap_usd', 'market_cap_eth'],
                'units': ['USD', 'ETH'],
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg'
            }
        }

        self.fees_list = ['txcosts_avg_eth', 'txcosts_native_median_eth', 'txcosts_median_eth']

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
        df_tmp['unix'] = df_tmp['unix'].mask(df_tmp['date'] == df_tmp['date'].min(), df_tmp['date'].dt.to_period("M").dt.start_time.astype(np.int64) // 10**6)

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

    def generate_fees_list(self, df, metric_key, origin_key, granularity, eth_price):
        ## filter df to granularity = 'hourly' and metric_key = metric
        df = df[(df.granularity == granularity) & (df.metric_key == metric_key) & (df.origin_key == origin_key)]
        ## order df_tmp by timestamp desc and only keep top 2000 rows
        df = df.sort_values(by='timestamp', ascending=False).head(2000)
        ## only keep columns unix, value_usd
        df = df[['unix', 'value']]
        ## calculate value_usd by multiplying value with eth_price
        df['value_usd'] = df['value'] * eth_price
        df.rename(columns={'value': 'value_eth'}, inplace=True)

        mk_list = df.values.tolist()
        mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list]

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
            where kpi."timestamp" >= '2021-01-01'
                and kpi.metric_key in ({"'" + "','".join(metric_keys) + "'"})
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
        chain_user_list = self.chains_list_in_api + ['multiple']
        metric_user_list = self.metrics_list + ['user_base_daily', 'user_base_weekly', 'user_base_monthly', 'waa', 'maa', 'aa_last30d', 'aa_last7d', 'cca_last7d_exclusive']

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
        if chain.aggregate_addresses == False:
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

        # filter df to date >= 2021-09-01
        df_tmp = df_tmp.loc[df_tmp.date >= '2021-09-01']
        
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
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{self.api_version}/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)


    ##### JSON GENERATION METHODS #####
    
    def create_chain_details_jsons(self, df):
        ## loop over all chains and generate a chain details json for all chains and with all possible metrics
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_api == False:
                print(f'..skipped: Chain details export for {origin_key}. API is set to False')
                continue

            metrics_dict = {}
            for metric in self.metrics:
                if metric in chain.exclude_metrics:
                    print(f'..skipped: Chain details export for {origin_key} - {metric}. Metric is excluded for this chain')
                    continue
                # if origin_key == 'ethereum' and metric in ['tvl', 'rent_paid', 'profit']:
                #     continue
                # if origin_key == 'imx' and metric in ['txcosts', 'fees', 'profit']:
                #     continue
                
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

            details_dict = {
                'data': {
                    'chain_id': origin_key,
                    'chain_name': chain.name,
                    'symbol': chain.symbol,
                    'metrics': metrics_dict
                }
            }

            details_dict = fix_dict_nan(details_dict, f'chains/{origin_key}')

            if self.s3_bucket == None:
                self.save_to_json(details_dict, f'chains/{origin_key}')
            else:
                upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/chains/{origin_key}', details_dict, self.cf_distribution_id)
            print(f'DONE -- Chain details export for {origin_key}')

    def create_metric_details_jsons(self, df):
        ## loop over all metrics and generate a metric details json for all metrics and with all possible chains

        for metric in self.metrics:
            chains_dict = {}    
            for chain in adapter_mapping:
                origin_key = chain.origin_key
                if chain.in_api == False:
                    print(f'..skipped: Metric details export for {origin_key}. API is set to False')
                    continue

                if metric in chain.exclude_metrics:
                    print(f'..skipped: Metric details export for {origin_key} - {metric}. Metric is excluded for this chain')
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

    def create_master_json(self, df_data):
        exec_string = "SELECT sub_category_key, main_category_name, sub_category_name, main_category_key FROM blockspace_category_mapping"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## create dict with main_category_key as key and main_category_name as value, same for sub_categories
        main_category_dict = {}
        sub_category_dict = {}
        for index, row in df.iterrows():
            main_category_dict[row['main_category_key']] = row['main_category_name']
            sub_category_dict[row['sub_category_key']] = row['sub_category_name']


        ## create main_category <> sub_category mapping dict
        df_mapping = df.groupby(['main_category_key']).agg({'sub_category_key': lambda x: list(x)}).reset_index()
        mapping_dict = {}
        for index, row in df_mapping.iterrows():
            mapping_dict[row['main_category_key']] = row['sub_category_key']

        ## create dict with all chain info
        chain_dict = {}
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_api == False:
                print(f'..skipped: Master json export for {origin_key}. API is set to False')
                continue

            chain_dict[origin_key] = {
                'name': chain.name,
                'deployment': chain.deployment,
                'name_short': chain.name_short,
                'description': chain.description,
                'da_layer': chain.da_layer,
                'symbol': chain.symbol,
                'bucket': chain.bucket,
                'technology': chain.technology,
                'purpose': chain.purpose,
                'launch_date': chain.launch_date,
                'website': chain.website,
                'twitter': chain.twitter,
                'block_explorer': chain.block_explorer,
                'rhino_listed': bool(getattr(chain, 'rhino_naming', None)),
                'rhino_naming': getattr(chain, 'rhino_naming', None)
            }

        master_dict = {
            'current_version' : self.api_version,
            'default_chain_selection' : self.get_default_selection(df_data),
            'chains' : chain_dict,
            'metrics' : self.metrics,
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
            
            landing_dict['data']['top_contracts'][f"{days}d"] = {
                "data": contracts[
                    ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount", "daa", "gas_fees_eth_change", "gas_fees_usd_change", "txcount_change", "daa_change", "prev_gas_fees_eth", "prev_gas_fees_usd", "prev_txcount", "prev_daa", "gas_fees_eth_change_percent", "gas_fees_usd_change_percent", "txcount_change_percent", "daa_change_percent"]
                ].values.tolist(),
                "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_eth", "gas_fees_usd", "txcount", "daa", "gas_fees_eth_change", "gas_fees_usd_change", "txcount_change", "daa_change", "prev_gas_fees_eth", "prev_gas_fees_usd", "prev_txcount", "prev_daa", "gas_fees_eth_change_percent", "gas_fees_usd_change_percent", "txcount_change_percent", "daa_change_percent"]
            }

        landing_dict = fix_dict_nan(landing_dict, 'landing_page')

        if self.s3_bucket == None:
            self.save_to_json(landing_dict, 'landing_page')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/landing_page', landing_dict, self.cf_distribution_id)
        print(f'DONE -- landingpage export')

    def create_fundamentals_json(self, df):
        df = df[['metric_key', 'origin_key', 'date', 'value']].copy()

        ## filter out all metric_keys that end with _eth
        df = df[~df.metric_key.str.endswith('_eth')]
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

    def create_mvp_dict(self):
        exec_string = f""" SELECT * FROM public.mvp_app_level """
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        ## date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        ## create dict
        mvp_dict = df.to_dict(orient='records')

        if self.s3_bucket == None:
            self.save_to_json(mvp_dict, 'mvp_dict')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/mvp_dict', mvp_dict, self.cf_distribution_id)

    def create_fees_json(self):
        df = self.download_data_fees(self.fees_list)

        fees_dict = {
            "chain_data" : {}
        } 

        ## loop over all chains and generate a fees json for all chains
        for chain in adapter_mapping:
            origin_key = chain.origin_key
            if chain.in_fees_api == False:
                print(f'..skipped: Fees export for {origin_key}. API is set to False')
                continue
            
            eth_price = self.db_connector.get_last_price_usd('ethereum')

            hourly_dict = {}
            min_10_dict = {}

            for metric_key in self.fees_list:
                ## generate metric_name which is metric_key without the last 4 characters
                metric_name = metric_key[:-4]
                generated = self.generate_fees_list(df, metric_key, origin_key, 'hourly', eth_price)
                hourly_dict[metric_name] = {
                    "types": generated[1],
                    "data": generated[0]
                }
            
            for metric_key in self.fees_list:
                ## generate metric_name which is metric_key without the last 4 characters
                metric_name = metric_key[:-4]
                generated = self.generate_fees_list(df, metric_key, origin_key, '10_min', eth_price)
                min_10_dict[metric_name] = {
                    "types": generated[1],
                    "data": generated[0]
                }

            fees_dict["chain_data"][origin_key] = {
                'hourly': hourly_dict,
                'ten_min': min_10_dict
            }

        fees_dict = fix_dict_nan(fees_dict, f'fees')

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, f'fees')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees', fees_dict, self.cf_distribution_id)
        print(f'DONE -- Fees export')

    def create_fees_dict(self):
        df = self.download_data_fees(self.fees_list)

        for adapter in adapter_mapping:
            ## filter out origin_keys from df if in_api=false
            if adapter.in_fees_api == False:
                #print(f"Filtering out origin_keys for adapter {adapter.name}")
                df = df[df.origin_key != adapter.origin_key]

        ## generate usd values
        eth_price = self.db_connector.get_last_price_usd('ethereum')
        df_usd = df.copy()
        df_usd['value'] = df_usd['value'] * eth_price
        df_usd['metric_key'] = df_usd['metric_key'].str.replace("_eth", "_usd")
        df = pd.concat([df, df_usd])

        ## drop column timestamp
        df = df.drop(columns=['timestamp'])

        ## create dict
        fees_dict = df.to_dict(orient='records')

        if self.s3_bucket == None:
            self.save_to_json(fees_dict, 'fees_dict')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/fees_dict', fees_dict, self.cf_distribution_id)

    def create_all_jsons(self):
        df = self.get_all_data()
        self.create_master_json(df)
        self.create_landingpage_json(df)

        self.create_chain_details_jsons(df)
        self.create_metric_details_jsons(df)
        
        self.create_fundamentals_json(df)
        self.create_contracts_json()
        self.create_mvp_dict()
