import os
import json
import datetime
import pandas as pd

from src.adapters.mapping import adapter_mapping, AdapterMapping
from src.misc.helper_functions import upload_json_to_cf_s3

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
            json.dump(data, fp)

    ##### JSON GENERATION METHODS #####

    def create_blockspace_overview_json(self):
        overview_dict = {
            "data": {
                "metric_id": "overview",
                "chains": {}
            }
        }

        # define the timeframes in days for the overview data
        overview_timeframes = [365, 180, 90, 30, 7]

        # get main_category_keys in the blockspace_df
        category_mapping = self.get_category_mapping()

        main_category_keys = category_mapping['main_category_key'].unique(
        ).tolist()

        # print("main_category_keys")
        # print(main_category_keys)

        # get all chains and add all_l2s to the list
        chain_keys = self.get_blockspace_chain_keys()

        # print("chain_keys")
        # print(chain_keys)

        for chain_key in chain_keys:
            # origin_key = chain_key
            if chain_key == 'ethereum':
                continue

            chain_df = self.download_chain_blockspace_overview_data(chain_key)

            # print(chain_key)
            # print(chain_df)

            # get the chain_name from the adapter_mapping array
            chain_name = [x.name for x in adapter_mapping if x.origin_key ==
                        chain_key][0] if chain_key != 'all_l2s' else 'All L2s'

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

            # create dict for each main_category_key
            # main_category_dict = {}
            for main_category_key in main_category_keys:
                # filter the dataframes accordingly, we'll use the chain_totals_df to calculate the gas_fees_share and txcount_share
                main_category_df = chain_df.loc[(
                    chain_df.main_category_key == main_category_key)]

                # create dict for each main_category_key
                chain_dict["daily"][main_category_key] = {
                    "data": []
                }

                # create list of lists for each main_category_key
                mk_list = main_category_df[[
                    'unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].values.tolist()

                # convert the first element of each list to int (unix timestamp)
                chain_dict["daily"][main_category_key]['data'] = mk_list

                for timeframe in overview_timeframes:
                    # create dict for each timeframe
                    if f'{timeframe}d' not in chain_dict["overview"]:
                        chain_dict["overview"][f'{timeframe}d'] = {}

                    # calculate averages for timeframe for this main_category_key
                    averages = main_category_df.loc[(
                        main_category_df.unix >= (datetime.datetime.now() - datetime.timedelta(days=timeframe)).timestamp() * 1000)][[
                            'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].mean()

                    # fill NaN values with 0
                    averages.fillna(0, inplace=True)

                    # if we have any non-zero values, add list of averages for each timeframe
                    if averages.sum() > 0:
                        chain_dict["overview"][f'{timeframe}d'][main_category_key] = averages.to_list(
                        )

                    # create list of lists for each timeframe
                    mk_list = main_category_df.loc[(
                        main_category_df.unix >= (datetime.datetime.now() - datetime.timedelta(days=timeframe)).timestamp() * 1000)][[
                            'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share_eth', 'gas_fees_share_usd', 'txcount_share']].values.tolist()

                    # convert the first element of each list to int (unix timestamp)
                    chain_dict["overview"][f'{timeframe}d']['data'] = mk_list
                    # get_top_contracts_by_category(self, category_type, category, chain, top_by, days):

                    chain_arg = chain_key if chain_key != 'all_l2s' else "all"

                    top_contracts_gas = self.db_connector.get_top_contracts_by_category('main_category', main_category_key, chain_arg, 'gas', timeframe)
                    # top_contracts['address] is [b'^', b'h', b'\xbe', b'\x9a', b'S', b'.', b'\... we need to convert it to a string and add 0x to the beginning
                    top_contracts_gas['address'] = top_contracts_gas['address'].apply(lambda x: '0x' + bytes(x).hex())

                    # print("top_contracts_gas")
                    # print(top_contracts_gas)

                    chain_dict["overview"][f'{timeframe}d']['contracts'] = {
                        "data": top_contracts_gas[
                            ['address', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "txcount"]
                        ].values.tolist(),
                        "types": ["address", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "txcount_absolute"]
                    }

            overview_dict['data']['chains'][chain_key] = chain_dict

        if self.s3_bucket == None:
            self.save_to_json(overview_dict, f'blockspace/overview')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/overview', overview_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for overview')

    def get_comparison_aggregate_data_day(self, days, category_type):
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
                    and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
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
                and date >= CURRENT_DATE - interval '{days} days'
                -- ignore total_usage
                and sub_category_key != 'total_usage'
            GROUP BY origin_key, bcm.main_category_key, {sub_category_key_str} t.total_gas_fees_eth, t.total_txcount
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        # export csv
        df.to_csv(f'./blockspace_comparison_{sub_category_key_str}_{days}d.csv', index=False)

        return df


    def get_comparison_aggregate_data_all(self):
        data = {}

        timeframes = [7, 30, 90, 365]

        for timeframe in timeframes:
            main_cat_df = self.get_comparison_aggregate_data_day(timeframe)


    def get_comparison_daily_data(self, days):
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
                    and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                    -- ignore total_usage
                    and sub_category_key != 'total_usage'
                GROUP BY origin_key, date
                
            ),
            -- Daily values for each chain over the specified days
            daily AS (
                SELECT 
                    origin_key, 
                    bcm.main_category_key,
                    sub_category_key,
                    date,
                    SUM(gas_fees_eth) AS gas_fees_eth,
                    SUM(gas_fees_usd) AS gas_fees_usd,
                    SUM(txcount) AS txcount
                FROM blockspace_fact_sub_category_level
                LEFT JOIN blockspace_category_mapping bcm USING (sub_category_key)
                WHERE date < DATE_TRUNC('day', NOW())
                    and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                    -- ignore total_usage
                    and sub_category_key != 'total_usage'
                GROUP BY origin_key, bcm.main_category_key, sub_category_key, date
                
            )

            -- Aggregate for each sub_category_key and chain over specified days
            SELECT
                origin_key,
                main_category_key,
                sub_category_key,
                EXTRACT(EPOCH FROM date)::bigint * 1000 AS unix,
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

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        # export csv
        df.to_csv(f'./blockspace_daily_{days}d.csv', index=False)

        return df

    def get_comparison_totals_per_chain_by_timeframe(self, timeframe):
        exec = f"""
            -- Total values for each chain over the specified days
            SELECT 
                origin_key,
                SUM(gas_fees_eth) AS gas_fees_eth,
                SUM(txcount) AS txcount
            FROM blockspace_fact_sub_category_level
            WHERE date < DATE_TRUNC('day', NOW())
                and date >= DATE_TRUNC('day', NOW() - INTERVAL '{timeframe} days')
                -- ignore total_usage
                and sub_category_key != 'total_usage'
            GROUP BY origin_key
        """

        df = pd.read_sql(exec, self.db_connector.engine)

        # fill NaN values with 0
        df.fillna(0, inplace=True)

        # export csv
        df.to_csv(f'./blockspace_totals_{timeframe}d.csv', index=False)

        return df



    def create_blockspace_comparison_json(self):
        # create base dict
        comparison_dict = {
            'data': {
                # keys for main categories
            }
        }

        # get data for each timeframe
        timeframes = [7, 30, 90, 365]

        # daily data for max timeframe
        sub_cat_daily_df = self.get_comparison_daily_data(timeframes[-1])
        main_cat_daily_df = self.get_comparison_daily_data(timeframes[-1])

        for timeframe in timeframes:
            timeframe_totals_df = self.get_comparison_totals_per_chain_by_timeframe(timeframe)
            sub_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'sub_category')
            main_cat_agg_df = self.get_comparison_aggregate_data_day(timeframe, 'main_category')
            
            timeframe_key = f'{timeframe}d'

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
                            "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                            # daily data for each chain
                        },
                        "type": "main_category"
                    }
                
                comparison_dict['data'][main_cat]['aggregated'][timeframe_key] = {
                    "contracts": {
                        "data":[
                            # .. top contracts

                        ],
                        "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                    },
                    "data": {
                        "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                        # .. data for each chain
                    }
                }

                # get top contracts for main category
                top_contracts_gas = self.db_connector.get_top_contracts_by_category('main_category', main_cat, 'all', 'gas', timeframe)

                if top_contracts_gas.shape[0] > 0:
                    # calculate txcount_share by dividing txcount by total_txcount for the origin_key
                    top_contracts_gas['txcount_share'] = top_contracts_gas.apply(lambda x: x['txcount'] / timeframe_totals_df[timeframe_totals_df['origin_key'] == x['origin_key']]['txcount'].values[0], axis=1)

                    # calculate gas_fees_share by dividing gas_fees_eth by total_gas_fees_eth for the origin_key
                    top_contracts_gas['gas_fees_share'] = top_contracts_gas.apply(lambda x: x['gas_fees_eth'] / timeframe_totals_df[timeframe_totals_df['origin_key'] == x['origin_key']]['gas_fees_eth'].values[0], axis=1)

                    # export csv
                    top_contracts_gas.to_csv(f'./blockspace_top_contracts_{main_cat}_{timeframe}d.csv', index=False)

                    # top_contracts['address] is [b'^', b'h', b'\xbe', b'\x9a', b'S', b'.', b'\... we need to convert it to a string and add 0x to the beginning
                    top_contracts_gas['address'] = top_contracts_gas['address'].apply(lambda x: '0x' + bytes(x).hex())

                    # only keep top 10
                    top_contracts_gas = top_contracts_gas.head(10)

                
                    # add top contracts to dict
                    comparison_dict['data'][main_cat]['aggregated'][timeframe_key]['contracts']['data'] = top_contracts_gas[['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "gas_fees_share", "txcount", "txcount_share"]].values.tolist()

                # add aggregate timefram edata for each chain for each main category
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
                                "types": ["unix", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                                # daily data for each chain
                            },
                            "type": "main_category"
                        }

                    comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key] = {
                        "contracts": {
                            "data":[
                                # .. top contracts

                            ],
                            "types": ["address", "project_name", "name", "main_category_key", "sub_category_key", "chain", "gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                        },
                        "data": {
                            "types": ["gas_fees_absolute_eth", "gas_fees_absolute_usd", "gas_fees_share", "txcount_absolute", "txcount_share"]
                            # .. data for each chain
                        }
                    }

                    # get top contracts for sub category
                    top_contracts_gas = self.db_connector.get_top_contracts_by_category('sub_category', sub_cat, 'all', 'gas', timeframe)

                    if top_contracts_gas.shape[0] > 0:
                        # calculate txcount_share by dividing txcount by total_txcount for the origin_key
                        top_contracts_gas['txcount_share'] = top_contracts_gas.apply(lambda x: x['txcount'] / timeframe_totals_df[timeframe_totals_df['origin_key'] == x['origin_key']]['txcount'].values[0], axis=1)

                        # calculate gas_fees_share by dividing gas_fees_eth by total_gas_fees_eth for the origin_key
                        top_contracts_gas['gas_fees_share'] = top_contracts_gas.apply(lambda x: x['gas_fees_eth'] / timeframe_totals_df[timeframe_totals_df['origin_key'] == x['origin_key']]['gas_fees_eth'].values[0], axis=1)
                        
                        # top_contracts['address] is [b'^', b'h', b'\xbe', b'\x9a', b'S', b'.', b'\... we need to convert it to a string and add 0x to the beginning
                        top_contracts_gas['address'] = top_contracts_gas['address'].apply(lambda x: '0x' + bytes(x).hex())

                        # only keep top 10
                        top_contracts_gas = top_contracts_gas.head(10)

                        # add top contracts to dict
                        comparison_dict['data'][main_cat]['subcategories'][sub_cat]['aggregated'][timeframe_key]['contracts']['data'] = top_contracts_gas[['address', 'project_name', 'contract_name', "main_category_key", "sub_category_key", "origin_key", "gas_fees_eth", "gas_fees_usd", "gas_fees_share", "txcount", "txcount_share"]].values.tolist()

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
                # print("daily main category chain_df", chain, chain_df)
                if chain_df.shape[0] > 0:
                    chain_df.sort_values(by=['unix'], inplace=True, ascending=True)
                    comparison_dict['data'][main_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'txcount', 'gas_fees_share', 'txcount_share']].values.tolist()

                # add daily data for each chain for each sub category of main category
                    for sub_cat in sub_cat_daily_df[sub_cat_daily_df['main_category_key'] == main_cat]['sub_category_key'].unique():
                        # print("daily sub category chain_df", chain, chain_df)
                        chain_df = sub_cat_daily_df[(sub_cat_daily_df['origin_key'] == chain) & (sub_cat_daily_df['sub_category_key'] == sub_cat)]
                        if chain_df.shape[0] > 0:
                            chain_df.sort_values(by=['unix'], inplace=True, ascending=True)
                            comparison_dict['data'][main_cat]['subcategories'][sub_cat]['daily'][chain] = chain_df[['unix', 'gas_fees_eth', 'gas_fees_usd', 'gas_fees_share', 'txcount', 'txcount_share']].values.tolist()

        if self.s3_bucket == None:
            self.save_to_json(comparison_dict, f'blockspace/category_comparison')
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/blockspace/overview', comparison_dict, self.cf_distribution_id)
        print(f'-- DONE -- Blockspace export for overview')

                
                
                


    def create_all_jsons(self):
        self.create_blockspace_comparison_json()
        self.create_blockspace_overview_json()
        