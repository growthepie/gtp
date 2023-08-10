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

    def create_all_jsons(self):
        self.create_blockspace_overview_json()
        