import os
import simplejson as json
import datetime
import pandas as pd

from src.adapters.mapping import adapter_mapping, AdapterMapping
from src.misc.helper_functions import upload_json_to_cf_s3, db_addresses_to_checksummed_addresses

class BlockspaceJSONCreation():

    def __init__(self, s3_bucket, cf_distribution_id, db_connector, api_version):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector

    def download_chain_blockspace_overview_data(self, chain_key):
        exec_string = f"""
            SELECT 
                bs_cm.main_category_key,
                bs_scl.origin_key as origin_key,
                bs_scl."date",
                bs_scl.gas_fees_eth,
                bs_scl.gas_fees_usd,
                bs_scl.txcount,
                gas_fees_eth / NULLIF(SUM(gas_fees_eth) over(partition by date), 0) AS gas_fees_share_eth,
                gas_fees_usd / NULLIF(SUM(gas_fees_usd) over(partition by date), 0) AS gas_fees_share_usd,
                txcount / NULLIF(SUM(txcount) over(partition by date), 0) AS txcount_share
            FROM public.blockspace_fact_sub_category_level bs_scl
            JOIN public.blockspace_category_mapping bs_cm on bs_scl.sub_category_key = bs_cm.sub_category_key
            WHERE bs_scl.origin_key = '{chain_key}'
                and bs_scl."date" >= '2021-01-01'
                and bs_scl."date" < date_trunc('day', now())
                -- ignore total_usage
                and bs_scl.sub_category_key != 'total_usage'
            ORDER BY bs_scl."date" ASC
        """

        if chain_key == 'all_l2s':
            exec_string = f"""
                SELECT
                    main_category_key,
                    by_category.origin_key as origin_key,
                    by_category."date",
                    by_category.gas_fees_eth,
                    by_category.gas_fees_usd,
                    by_category.txcount,
                    by_category.gas_fees_eth / NULLIF(totals.gas_fees_eth_total, 0) AS gas_fees_share_eth,
                    by_category.gas_fees_usd / NULLIF(totals.gas_fees_usd_total, 0) AS gas_fees_share_usd,
                    by_category.txcount / NULLIF(totals.txcount_total, 0) AS txcount_share 
                FROM
                (
                    SELECT
                        'all_l2s' AS origin_key,
                        bs_cm.main_category_key,
                        bs_scl."date",
                        SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                        SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                        SUM(bs_scl.txcount) AS txcount 
                    FROM
                        public.blockspace_fact_sub_category_level bs_scl 
                    JOIN
                        public.blockspace_category_mapping bs_cm 
                        ON bs_scl.sub_category_key = bs_cm.sub_category_key 
                    WHERE
                        bs_scl."date" >= '2021-01-01' 
                        AND bs_scl."date" < date_trunc('day', NOW()) 
                        -- ignore total_usage
                        and bs_scl.sub_category_key != 'total_usage'
                    GROUP BY
                        bs_scl."date",
                        bs_cm.main_category_key 
                    ORDER BY
                        bs_scl."date" DESC
                )
                by_category 
                JOIN
                    (
                    SELECT
                        'all_l2s' AS origin_key,
                        bs_scl."date",
                        SUM(bs_scl.gas_fees_eth) AS gas_fees_eth_total,
                        SUM(bs_scl.gas_fees_usd) AS gas_fees_usd_total,
                        SUM(bs_scl.txcount) AS txcount_total 
                    FROM
                        public.blockspace_fact_sub_category_level bs_scl 
                        JOIN
                        public.blockspace_category_mapping bs_cm 
                        ON bs_scl.sub_category_key = bs_cm.sub_category_key 
                    WHERE
                        bs_scl."date" >= '2021-01-01' 
                        AND bs_scl."date" < date_trunc('day', NOW()) 
                        -- ignore total_usage
                        and bs_scl.sub_category_key != 'total_usage'
                    GROUP BY
                        bs_scl."date" 
                    )
                    totals 
                    ON by_category.date = totals.date 
                WHERE
                by_category."date" >= '2021-01-01' 
                AND by_category."date" < date_trunc('day', NOW()) 
                ORDER BY
                by_category."date" ASC
            """

        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        # date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        # datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        # drop date column
        df.drop(columns=['date'], inplace=True)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        return df
    
    def get_blockspace_overview_daily_data(self, chain_keys):
        where_origin_key = f"AND bs_scl.origin_key = '{chain_keys[0]}'" if len(chain_keys) == 1 else "AND bs_scl.origin_key IN ('" + "','".join(chain_keys) + "')"
        select_origin_key = f"bs_scl.origin_key as chain_key" if len(chain_keys) == 1 else "'all_l2s' as chain_key"
        group_by_origin_key = f"bs_scl.origin_key" if len(chain_keys) == 1 else "chain_key"
        
        exec = f"""
            WITH totals AS (
                SELECT
                    bs_scl.date,
                    {select_origin_key},
                    SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                    SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                    SUM(bs_scl.txcount) AS txcount
                FROM
                    public.blockspace_fact_sub_category_level bs_scl
                WHERE
                    bs_scl."date" < date_trunc('day', NOW())
                    AND bs_scl.sub_category_key != 'total_usage'
                    {where_origin_key}
                GROUP BY
                    bs_scl."date",
                    {group_by_origin_key}
            )
            SELECT
                bs_scl.date,
                {select_origin_key},
                bs_cm.main_category_key,
                SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                SUM(bs_scl.txcount) AS txcount,
                SUM(bs_scl.gas_fees_eth) / NULLIF(totals.gas_fees_eth, 0) AS gas_fees_share_eth,
                SUM(bs_scl.gas_fees_usd) / NULLIF(totals.gas_fees_usd, 0) AS gas_fees_share_usd,
                SUM(bs_scl.txcount) / NULLIF(totals.txcount, 0) AS txcount_share
            FROM
                public.blockspace_fact_sub_category_level bs_scl
            JOIN
                public.blockspace_category_mapping bs_cm
                ON bs_scl.sub_category_key = bs_cm.sub_category_key
            JOIN 
                totals 
                ON bs_scl.date = totals.date
            WHERE
                bs_scl."date" < date_trunc('day', NOW())
                AND bs_scl.sub_category_key != 'total_usage'
                {where_origin_key}
            GROUP BY
                bs_scl."date",
                {group_by_origin_key},
                bs_cm.main_category_key,
                totals.gas_fees_eth,
                totals.gas_fees_usd,
                totals.txcount
            ORDER BY
                bs_scl."date" ASC
        """

        df = pd.read_sql(exec, self.db_connector.engine.connect())

        # date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        # datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        # drop date column
        df.drop(columns=['date'], inplace=True)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        return df
    
    def get_blockspace_overview_timeframe_overview(self, chain_keys, days):
        date_where = f"AND bs_scl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
        where_origin_key = f"AND bs_scl.origin_key = '{chain_keys[0]}'" if len(chain_keys) == 1 else "AND bs_scl.origin_key IN ('" + "','".join(chain_keys) + "')"
        select_origin_key = f"bs_scl.origin_key as chain_key" if len(chain_keys) == 1 else "'all_l2s' as chain_key"
        group_by_origin_key = f"bs_scl.origin_key" if len(chain_keys) == 1 else "chain_key"
        
        exec = f"""
            WITH totals AS (
                SELECT
                    {select_origin_key},
                    SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                    SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                    SUM(bs_scl.txcount) AS txcount
                FROM
                    public.blockspace_fact_sub_category_level bs_scl
                WHERE
                    bs_scl."date" < date_trunc('day', NOW())
                    {date_where}
                    AND bs_scl.sub_category_key != 'total_usage'
                    {where_origin_key}
                GROUP BY
                    {group_by_origin_key}
            )

                
            SELECT
                {select_origin_key},
                bs_cm.main_category_key,
                SUM(bs_scl.gas_fees_eth) AS gas_fees_eth,
                SUM(bs_scl.gas_fees_usd) AS gas_fees_usd,
                SUM(bs_scl.txcount) AS txcount,
                SUM(bs_scl.gas_fees_eth) / NULLIF(totals.gas_fees_eth, 0) AS gas_fees_share_eth,
                SUM(bs_scl.gas_fees_usd) / NULLIF(totals.gas_fees_usd, 0) AS gas_fees_share_usd,
                SUM(bs_scl.txcount) / NULLIF(totals.txcount, 0) AS txcount_share
            FROM
                public.blockspace_fact_sub_category_level bs_scl
            JOIN
                public.blockspace_category_mapping bs_cm
                ON bs_scl.sub_category_key = bs_cm.sub_category_key
            JOIN
                totals
                ON chain_key = totals.chain_key
            WHERE
                bs_scl."date" < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and bs_scl.sub_category_key != 'total_usage'
                {where_origin_key}
            GROUP BY
                bs_cm.main_category_key,
                {group_by_origin_key},
                totals.gas_fees_eth,
                totals.gas_fees_usd,
                totals.txcount
            ORDER BY
                bs_cm.main_category_key ASC
        """

        df = pd.read_sql(exec, self.db_connector.engine.connect())

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        return df


    def get_category_mapping(self):
        exec_string = "SELECT * FROM public.blockspace_category_mapping"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())
        return df

    def get_blockspace_chain_keys(self):
        exec_string = "SELECT DISTINCT origin_key FROM public.blockspace_fact_sub_category_level"
        df = pd.read_sql(exec_string, self.db_connector.engine.connect())

        chain_key_list = df['origin_key'].tolist()
        chain_key_list.append('all_l2s')

        return chain_key_list
    
    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{self.api_version}/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)

    ##### JSON GENERATION METHODS #####

    def create_blockspace_overview_json(self):
        overview_dict = {
            "data": {
                "metric_id": "overview",
                "chains": {}
            }
        }

        # define the timeframes in days for the overview data
        overview_timeframes = [7, 30, 90, 180, 365, "max"]

        # get main_category_keys in the blockspace_df
        category_mapping = self.get_category_mapping()

        main_category_keys = category_mapping['main_category_key'].unique().tolist()

        # get all chains and add all_l2s to the list
        adapter_multi_mapping = adapter_mapping + [AdapterMapping(origin_key='all_l2s', name='All L2s', in_api=True, technology='-', purpose='-')]

        ## put all origin_keys from adapter_mapping in a list where in_api is True
        chain_keys = [chain.origin_key for chain in adapter_mapping if chain.in_api == True]

        #for chain_key in adapter_mapping:
        for chain in adapter_multi_mapping:
            origin_key = chain.origin_key
            if origin_key == 'ethereum':
                continue
            if chain.in_api == False:
                print(f'-- SKIPPED -- Chain blockspace overview export for {origin_key}. API is set to False')
                continue

            print(f"Processing {origin_key}")

            # get daily data (multiple rows per day - one for each main_category_key)
            if origin_key == 'all_l2s':
                chain_df = self.get_blockspace_overview_daily_data(chain_keys)
            else:
                chain_df = self.get_blockspace_overview_daily_data([origin_key])

            chain_timeframe_overview_dfs = {}
            for timeframe in overview_timeframes:
                if origin_key == 'all_l2s':
                    chain_timeframe_overview_dfs[timeframe] = self.get_blockspace_overview_timeframe_overview([origin_key], timeframe)
                else:
                    chain_timeframe_overview_dfs[timeframe] = self.get_blockspace_overview_timeframe_overview(chain_keys, timeframe)

            chain_name = chain.name

            # create dict for each chain
            chain_dict = {
                "chain_name": chain_name,
                "daily": {
                    "types": [
                        "unix",
                        "gas_fees_eth_absolute",
                        "gas_fees_usd_absolute",
                        "txcount_absolute",
                        "gas_fees_share_eth",
                        "gas_fees_share_usd",
                        "txcount_share"
                    ],
                    # data for main categories will be added in the for loop below
                },
                "overview": {
                    "types": [
                        "gas_fees_eth_absolute",
                        "gas_fees_usd_absolute",
                        "txcount_absolute",
                        "gas_fees_share_eth",
                        "gas_fees_share_usd",
                        "txcount_share"
                    ],
                    # data for timeframes will be added in the for loop below
                }
            }

            for main_category_key in main_category_keys:
                # filter the dataframes accordingly, we'll use the chain_totals_df to calculate the gas_fees_share and txcount_share
                main_category_df = chain_df.loc[(chain_df.main_category_key == main_category_key)]

                # create dict for each main_category_key
                chain_dict["daily"][main_category_key] = {
                    "data": []
                }

                # create list of lists for each main_category_key
                mk_list = main_category_df[[
                    'unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].values.tolist()

                # add the list of lists to the main_category_key dict
                chain_dict["daily"][main_category_key]['data'] = mk_list

                for timeframe in overview_timeframes:
                    timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

                    # create dict for each timeframe
                    if timeframe_key not in chain_dict["overview"]:
                        chain_dict["overview"][timeframe_key] = {}

                    if main_category_key not in chain_dict["overview"][timeframe_key]:
                        chain_dict["overview"][timeframe_key][main_category_key] = {}

                    # get the averages for timeframe and main_category_key
                    averages = chain_timeframe_overview_dfs[timeframe].loc[(chain_timeframe_overview_dfs[timeframe].main_category_key == main_category_key)][[
                        'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']]

                    # if we have any non-zero values, add list of averages for each timeframe
                    if averages.any().any():
                        chain_dict["overview"][timeframe_key][main_category_key]['data'] = averages.values.tolist()[0]
                    

                    ## only add contracts to all field in dicts
                    if origin_key == 'all_l2s':
                        top_contracts_gas = self.db_connector.get_contracts_overview(main_category_key, timeframe)
                        # convert address to checksummed string 
                        top_contracts_gas = db_addresses_to_checksummed_addresses(top_contracts_gas, ['address'])

                        chain_dict["overview"][timeframe_key][main_category_key]['contracts'] = {
                            "data": top_contracts_gas[
                                ['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]
                            ].values.tolist(),
                            "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                        }


            overview_dict['data']['chains'][origin_key] = chain_dict

        if self.s3_bucket == None:
            self.save_to_json(overview_dict, f'blockspace/overview')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/overview', overview_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for overview')

    def get_comparison_aggregate_data_day(self, days, category_type):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""
    
        sub_category_key_str = ""
        if category_type == 'sub_category':
            sub_category_key_str = "sub_category_key, "          

        exec = f"""
            -- Total values for each chain (origin_key) over specified days
            WITH totals AS (
                SELECT 
                    origin_key, 
                    SUM(gas_fees_eth) AS total_gas_fees_eth,
                    SUM(txcount) AS total_txcount
                FROM blockspace_fact_sub_category_level
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and sub_category_key != 'total_usage'
                GROUP BY origin_key
            )

            -- Aggregate for each sub_category_key and chain over specified days
            SELECT 
                origin_key, 
                bcm.main_category_key,
                {sub_category_key_str}
                SUM(gas_fees_eth) AS gas_fees_eth,
                SUM(gas_fees_usd) AS gas_fees_usd,
                SUM(txcount) AS txcount,
                (SUM(gas_fees_eth) / t.total_gas_fees_eth) AS gas_fees_share,
                (SUM(txcount) / t.total_txcount) AS txcount_share
            FROM blockspace_fact_sub_category_level
            JOIN totals t USING (origin_key)
            LEFT JOIN blockspace_category_mapping bcm USING (sub_category_key)
            WHERE date < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and sub_category_key != 'total_usage'
            GROUP BY origin_key, bcm.main_category_key, {sub_category_key_str} t.total_gas_fees_eth, t.total_txcount
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        return df

    def get_comparison_daily_data(self, days, category_type):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ""

        sub_category_key_str = ""
        if category_type == 'sub_category':
            sub_category_key_str = "sub_category_key, "  

        exec = f"""
            -- Total values for each chain over the specified days
            WITH totals AS (
                SELECT 
                    date,
                    origin_key, 
                    SUM(gas_fees_eth) AS total_gas_fees_eth,
                    SUM(txcount) AS total_txcount
                FROM blockspace_fact_sub_category_level
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and sub_category_key != 'total_usage'
                GROUP BY origin_key, date
                
            ),
            -- Daily values for each chain over the specified days
            daily AS (
                SELECT 
                    origin_key, 
                    bcm.main_category_key,
                    {sub_category_key_str}
                    date,
                    SUM(gas_fees_eth) AS gas_fees_eth,
                    SUM(gas_fees_usd) AS gas_fees_usd,
                    SUM(txcount) AS txcount
                FROM blockspace_fact_sub_category_level
                LEFT JOIN blockspace_category_mapping bcm USING (sub_category_key)
                WHERE date < DATE_TRUNC('day', NOW())
                    {date_where}
                    -- ignore total_usage
                    and sub_category_key != 'total_usage'
                GROUP BY origin_key, bcm.main_category_key, {sub_category_key_str} date
                
            )

            -- Aggregate for each sub_category_key and chain over specified days
            SELECT
                origin_key,
                main_category_key,
                {sub_category_key_str}
                date,
                gas_fees_eth,
                gas_fees_usd,
                txcount,
                -- 0 if total_gas_fees_eth is 0
                CASE WHEN t.total_gas_fees_eth = 0 THEN 0 ELSE (gas_fees_eth / t.total_gas_fees_eth) END AS gas_fees_share,
                CASE WHEN t.total_txcount = 0 THEN 0 ELSE (txcount / t.total_txcount) END AS txcount_share
            FROM daily
            JOIN totals t USING (origin_key, date)
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # date to datetime column in UTC
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')

        # datetime to unix timestamp using timestamp() function
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        # set index to unix timestamp but keep unix column
        df.set_index('unix', inplace=True, drop=False)

        # sort by unix timestamp
        df.sort_index(inplace=True, ascending=True)

        return df

    def get_comparison_totals_per_chain_by_timeframe(self, timeframe):
        date_where = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{timeframe} days')" if timeframe != 'max' else ""

        exec = f"""
            -- Total values for each chain over the specified days
            SELECT 
                origin_key,
                SUM(gas_fees_eth) AS gas_fees_eth,
                SUM(txcount) AS txcount
            FROM blockspace_fact_sub_category_level
            WHERE date < DATE_TRUNC('day', NOW())
                {date_where}
                -- ignore total_usage
                and sub_category_key != 'total_usage'
            GROUP BY origin_key
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        return df


    ### ToDo: Add check/filters whether chain data should be loaded based on in_api flag
    def create_blockspace_comparison_json(self):
        # create base dict
        comparison_dict = {
            'data': {
                # keys for main categories
            }
        }

        # get data for each timeframe
        timeframes = [7, 30, 180, "max"]

        # daily data for max timeframe
        sub_cat_daily_df = self.get_comparison_daily_data(timeframes[-1], 'sub_category')
        main_cat_daily_df = self.get_comparison_daily_data(timeframes[-1], 'main_category')

        for timeframe in timeframes:
            sub_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'sub_category')
            main_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'main_category')
            
            timeframe_key = f'{timeframe}d' if timeframe != 'max' else 'max'

            # create dict for each main category
            for main_cat in main_cat_agg_df['main_category_key'].unique():
                if main_cat not in comparison_dict['data']:
                    comparison_dict['data'][main_cat] = {
                        "aggregated": {
                            # .. keys for timeframes
                        },
                        "subcategories": {
                            # keys for sub categories
                            "list": sub_cat_agg_df[sub_cat_agg_df['main_category_key'] == main_cat]['sub_category_key'].unique().tolist() if main_cat in sub_cat_agg_df['main_category_key'].unique() else [],
                        },
                        "daily": {
                            "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                            # daily data for each chain
                        },
                        "daily_7d_rolling":{
                            "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                            # daily data for each chain
                        },
                        "type": "main_category"
                    }
                
                comparison_dict['data'][main_cat]['aggregated'][timeframe_key] = {
                    "contracts": {
                        "data":[
                            # .. top contracts

                        ],
                        "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                    
                    },
                    "data": {
                        "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                        # .. data for each chain
                    }
                }

                # get top contracts for main category
                top_contracts_gas = self.db_connector.get_contracts_category_comparison(main_cat, timeframe)
                # convert address to checksummed string 
                top_contracts_gas = db_addresses_to_checksummed_addresses(top_contracts_gas, ['address'])

                if top_contracts_gas.shape[0] > 0:
                    ## Trim values (smaller json size)

                    # for values in columns "gas_fees_usd" keep only 2 decimals
                    top_contracts_gas['gas_fees_usd'] = top_contracts_gas['gas_fees_usd'].apply(lambda x: round(x, 2))
                    # for values in columns "gas_fees_eth" keep only 5 decimals
                    top_contracts_gas['gas_fees_eth'] = top_contracts_gas['gas_fees_eth'].apply(lambda x: round(x, 5))                                        

                    # add top contracts to dict
                    comparison_dict['data'][main_cat]['aggregated'][timeframe_key]['contracts']['data'] = top_contracts_gas[['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]].values.tolist()


                # add aggregate timeframe data for each chain for each main category
                for chain in main_cat_agg_df['origin_key'].unique():
                    chain_df = main_cat_agg_df[(main_cat_agg_df['origin_key'] == chain) & (main_cat_agg_df['main_category_key'] == main_cat)]
                    if chain_df.shape[0] > 0:
                        comparison_dict['data'][main_cat]['aggregated'][timeframe_key]['data'][chain] = chain_df[['gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share', 'txcount_share']].values.tolist()[0]

                # create dict for each sub category of main category
                for sub_cat in sub_cat_agg_df[sub_cat_agg_df['main_category_key'] == main_cat]['sub_category_key'].unique():
                    if sub_cat not in  comparison_dict['data'][main_cat]['subcategories']:
                        comparison_dict['data'][main_cat]['subcategories'][sub_cat] = {
                            "aggregated": {
                                # .. keys for timeframes
                            },
                            "daily": {
                                "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                                # daily data for each chain
                            },
                            "daily_7d_rolling":{
                                "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                                # daily data for each chain
                            },
                            "type": "sub_category"
                        }

                    comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key] = {
                        "data": {
                            "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                            # .. data for each chain
                        }
                    }

                    # add aggregate timeframe data for each chain for each sub category
                    for chain in sub_cat_agg_df['origin_key'].unique():
                        chain_df = sub_cat_agg_df[(sub_cat_agg_df['origin_key'] == chain) & (sub_cat_agg_df['sub_category_key'] == sub_cat)]
                        if chain_df.shape[0] > 0:
                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key]['data'][chain] = chain_df[['gas_fees_eth', 'gas_fees_usd', 'gas_fees_share', 'txcount', 'txcount_share']].values.tolist()[0]
        
        # add daily data for each chain for each main category
        for chain in main_cat_daily_df['origin_key'].unique():
            for main_cat in main_cat_daily_df['main_category_key'].unique():
                # get daily data for each chain for each main category and sort by unix asc
                chain_df = main_cat_daily_df[(main_cat_daily_df['origin_key'] == chain) & (main_cat_daily_df['main_category_key'] == main_cat)]

                if chain_df.shape[0] > 0:
                    ## for values in column 'gas_fees_eth' keep only 5 decimals
                    chain_df['gas_fees_eth'] = chain_df['gas_fees_eth'].apply(lambda x: round(x, 5))
                    ## for values in column 'gas_fees_usd' keep only 2 decimals
                    chain_df['gas_fees_usd'] = chain_df['gas_fees_usd'].apply(lambda x: round(x, 2))
                    
                    comparison_dict['data'][main_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()
                    
                    # calculate rolling 7d average
                    chain_df['7d_gas_fees_eth'] = chain_df['gas_fees_eth'].rolling(7).mean()
                    chain_df['7d_gas_fees_usd'] = chain_df['gas_fees_usd'].rolling(7).mean()
                    chain_df['7d_txcount'] = chain_df['txcount'].rolling(7).mean()

                    # drop nan values
                    chain_df.dropna(inplace=True)
                    
                    ## for values in column 'gas_fees_eth' keep only 5 decimals
                    chain_df['7d_gas_fees_eth'] = chain_df['7d_gas_fees_eth'].apply(lambda x: round(x, 5))
                    ## for values in column 'gas_fees_usd' keep only 2 decimals
                    chain_df['7d_gas_fees_usd'] = chain_df['7d_gas_fees_usd'].apply(lambda x: round(x, 2))
                    
                    comparison_dict['data'][main_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_txcount']].values.tolist()

                    # add daily data for each chain for each sub category of main category
                    for sub_cat in sub_cat_daily_df[sub_cat_daily_df['main_category_key'] == main_cat]['sub_category_key'].unique():
                        # print("daily sub category chain_df", chain, chain_df)
                        chain_df = sub_cat_daily_df[(sub_cat_daily_df['origin_key'] == chain) & (sub_cat_daily_df['sub_category_key'] == sub_cat)]
                        
                        if chain_df.shape[0] > 0:
                            ## for values in column 'gas_fees_eth' keep only 5 decimals
                            chain_df['gas_fees_eth'] = chain_df['gas_fees_eth'].apply(lambda x: round(x, 5))
                            ## for values in column 'gas_fees_usd' keep only 2 decimals
                            chain_df['gas_fees_usd'] = chain_df['gas_fees_usd'].apply(lambda x: round(x, 2))

                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount']].values.tolist()
                            
                            # calculate rolling 7d average
                            chain_df['7d_gas_fees_eth'] = chain_df['gas_fees_eth'].rolling(7).mean()
                            chain_df['7d_gas_fees_usd'] = chain_df['gas_fees_usd'].rolling(7).mean()
                            chain_df['7d_txcount'] = chain_df['txcount'].rolling(7).mean()

                            # drop nan values
                            chain_df.dropna(inplace=True)

                            ## for values in column 'gas_fees_eth' keep only 5 decimals
                            chain_df['7d_gas_fees_eth'] = chain_df['7d_gas_fees_eth'].apply(lambda x: round(x, 5))
                            ## for values in column 'gas_fees_usd' keep only 2 decimals
                            chain_df['7d_gas_fees_usd'] = chain_df['7d_gas_fees_usd'].apply(lambda x: round(x, 2))

                            #comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_gas_fees_share', '7d_txcount', '7d_txcount_share']].values.tolist()
                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily_7d_rolling'][chain] = chain_df[['unix', '7d_gas_fees_eth', '7d_gas_fees_usd', '7d_txcount']].values.tolist()

        if self.s3_bucket == None:
            self.save_to_json(comparison_dict, f'blockspace/category_comparison')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/category_comparison', comparison_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for category_comparison')

    def create_all_jsons(self):
        self.create_blockspace_overview_json()
        self.create_blockspace_comparison_json()