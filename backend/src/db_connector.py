from pangres import upsert
import sqlalchemy
import pandas as pd

from dotenv import load_dotenv
load_dotenv() 
import os

db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")

class DbConnector:
        def __init__(self, db_user=db_user, db_passwd=db_passwd, db_host=db_host, db_name=db_name):
            print(f"Connecting to {db_user}@{db_host}")
            self.url = f"postgresql+psycopg2://{db_user}:{db_passwd}@{db_host}/{db_name}"
            self.engine = sqlalchemy.create_engine(
                self.url,
                connect_args={
                        "keepalives": 1,
                        "keepalives_idle": 30,
                        "keepalives_interval": 10,
                        "keepalives_count": 5,
                },
                pool_size=20, max_overflow=20
        )

        def upsert_table(self, table_name:str, df:pd.DataFrame, if_exists='update'):
                batch_size = 100000
                if df.shape[0] > 0:
                        if df.shape[0] > batch_size:
                                print(f"Batch upload necessary. Total size: {df.shape[0]}")
                                total_length = df.shape[0]
                                batch_start = 0
                                while batch_start < total_length:
                                        batch_end = batch_start + batch_size
                                        upsert(con=self.engine, df=df.iloc[batch_start:batch_end], table_name=table_name, if_row_exists=if_exists, create_table=False)
                                        print("Batch " + str(batch_end))
                                        batch_start = batch_end
                        else:
                                upsert(con=self.engine, df=df, table_name=table_name, if_row_exists='update', create_table=False)
                        return df.shape[0]
                
# ------------------------- additional db functions -------------------------

        def get_max_date(self, metric_key:str, origin_key:str):
                exec_string = f"SELECT MAX(date) as val FROM fact_kpis WHERE metric_key = '{metric_key}' AND origin_key = '{origin_key}';"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        val = row['val']
                return val
        
        def get_blockspace_max_date(self, origin_key:str):
                if origin_key == 'imx':
                        exec_string = f"SELECT MAX(date) as val FROM blockspace_fact_sub_category_level WHERE origin_key = '{origin_key}';"                        
                else:
                        exec_string = f"SELECT MAX(date) as val FROM blockspace_fact_contract_level WHERE origin_key = '{origin_key}';"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        val = row['val']
                return val
        
        def get_max_block(self, table_name:str):
                exec_string = f"SELECT MAX(block_number) as val FROM {table_name};"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        val = row['val']
                
                if val == None:
                        return 0
                else:
                        return val
        
        def get_values_in_eth(self, raw_metrics, days): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(raw_metrics) + "'"
                print(f"load usd values for : {mk_string}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'rent_paid_usd' THEN 'rent_paid_eth'
                                        WHEN 'fees_paid_usd' THEN 'fees_paid_eth'
                                        WHEN 'profit_usd' THEN 'profit_eth'
                                        WHEN 'tvl' THEN 'tvl_eth'
                                        WHEN 'stables_mcap' THEN 'stables_mcap_eth' 
                                        WHEN 'txcosts_median_usd' THEN 'txcosts_median_eth'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value / p.value as value
                        FROM fact_kpis tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                AND tkd.date < date_trunc('day', NOW()) 
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_latest_imx_refresh_date(self, tbl_name):
                if tbl_name == 'imx_orders':
                        exec_string = f"SELECT MAX(updated_timestamp) as last_refresh FROM {tbl_name};"
                else:
                        exec_string = f"SELECT MAX(timestamp) as last_refresh FROM {tbl_name};"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        last_refresh = str(row['last_refresh'])

                if last_refresh == 'None':
                        return '2021-01-01 00:00:00.000000'
                else:
                        return last_refresh
                
        def get_metric_sources(self, metric_key:str, origin_keys:list):
                if len(origin_keys) == 0:
                        exec_string = f'''
                                SELECT DISTINCT source
                                FROM metric_sources
                                WHERE metric_key = '{metric_key}'
                        '''
                else:
                        ok_string = "'" + "', '".join(origin_keys) + "'"
                        exec_string = f'''
                                SELECT DISTINCT source
                                FROM metric_sources
                                WHERE metric_key = '{metric_key}'
                                        AND origin_key in ({ok_string})
                        '''
                        # print (exec_string)
                

                df = pd.read_sql(exec_string, self.engine.connect())
                return df['source'].to_list()
        

        def get_blockspace_contracts(self, chain, days):
                ## Mantle stores fees in MNT: hence different logic for gas_fees_eth and gas_fees_usd
                if chain == 'mantle':
                        additional_cte = """
                                , mnt_price AS (
                                        SELECT "date", price_usd as value
                                        FROM public.prices_daily
                                        WHERE token_symbol = 'MNT'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN mnt_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        {additional_cte}

                        select
                                to_address as address,
                                date_trunc('day', block_timestamp) as date,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa,
                                '{chain}' as origin_key
                        from {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and empty_input = false -- we don't have to store addresses that received native transfers
                                and tx_fee > 0 -- no point in counting txs with 0 fees (most likely system tx)
                                and to_address <> '' 
                                and to_address is not null -- filter out contract creations arbitrum, optimism
                                and to_address <> '\\x0000000000000000000000000000000000008006' -- filter out contract creations zksync
                                and to_address <> 'None' -- filter out zora and pgn contract creation
                        group by 1,2
                        having count(*) > 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_native_transfers(self, chain, days):
                ## Mantle stores fees in MNT: hence different logic for gas_fees_eth and gas_fees_usd
                if chain == 'mantle':
                        additional_cte = """
                                , mnt_price AS (
                                        SELECT "date", price_usd as value
                                        FROM public.prices_daily
                                        WHERE token_symbol = 'MNT'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN mnt_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''


                ## native transfers: all transactions that have no input data
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'native_transfer' as sub_category_key,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and empty_input = true
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_contract_deplyments(self, chain, days):
                ## Mantle stores fees in MNT: hence different logic for gas_fees_eth and gas_fees_usd
                if chain == 'mantle':
                        additional_cte = """
                                , mnt_price AS (
                                        SELECT "date", price_usd as value
                                        FROM public.prices_daily
                                        WHERE token_symbol = 'MNT'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN mnt_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''

                if chain == 'zksync_era':
                        filter_string = "and to_address = '\\x0000000000000000000000000000000000008006'"
                elif chain == 'polygon_zkevm':
                        filter_string = "and receipt_contract_address is not null"
                else:
                        filter_string = "and to_address = '' or to_address is null"

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'smart_contract_deployment' as sub_category_key,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                {filter_string}
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_total(self, chain, days):
                ## Mantle stores fees in MNT: hence different logic for gas_fees_eth and gas_fees_usd
                if chain == 'mantle':
                        additional_cte = """
                                , mnt_price AS (
                                        SELECT "date", price_usd as value
                                        FROM public.prices_daily
                                        WHERE token_symbol = 'MNT'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN mnt_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
                else:
                        additional_cte = ''
                        tx_fee_eth_string = 'tx_fee'
                        tx_fee_usd_string = 'tx_fee * p.value'                        
                        additional_join = ''

                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )
                        {additional_cte}

                        select 
                                date_trunc('day', block_timestamp) as date,
                                'total_usage' as sub_category_key,
                                '{chain}' as origin_key,
                                sum({tx_fee_eth_string}) as gas_fees_eth,
                                sum({tx_fee_usd_string}) as gas_fees_usd, 
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        from {chain}_tx tx
                        left join eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        {additional_join}
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and tx_fee > 0 -- no point in counting txs with 0 fees (most likely system tx)
                        group by 1
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_sub_categories(self, chain, days):
                exec_string = f'''
                        SELECT 
                                lower(bl.sub_category_key) as sub_category_key,
                                '{chain}' as origin_key,
                                date,
                                sum(gas_fees_eth) as gas_fees_eth,
                                sum(gas_fees_usd) as gas_fees_usd,
                                sum(txcount) as txcount,
                                sum(daa) as daa
                        FROM public.blockspace_fact_contract_level cl
                        inner join blockspace_labels bl on cl.address = bl.address
                        where date < DATE_TRUNC('day', NOW())
                                and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and bl.origin_key = '{chain}' 
                                and cl.origin_key = '{chain}'
                        group by 1,2,3
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_unlabeled(self, chain, days):
                exec_string = f'''
                        with labeled_usage as (
                                SELECT 
                                        date,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount
                                FROM public.blockspace_fact_sub_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and sub_category_key <> 'unlabeled' and sub_category_key <> 'total_usage'
                                group by 1
                        ),
                        total_usage as (
                                SELECT 
                                        date,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount
                                FROM public.blockspace_fact_sub_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and sub_category_key = 'total_usage'
                                group by 1
                        )

                        select 
                                t.date,
                                'unlabeled' as sub_category_key,
                                '{chain}' as origin_key,
                                t.gas_fees_eth - l.gas_fees_eth as gas_fees_eth,
                                t.gas_fees_usd - l.gas_fees_usd as gas_fees_usd,
                                t.txcount - l.txcount as txcount
                        from total_usage t
                        left join labeled_usage l on t.date = l.date
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # """
        # DEPRECATED (currently not used) - 10.11.23
        # This function is used to get the top contracts by category for the blockspace dashboard
        # category_type: main_category or sub_category
        # category: the category key (e.g. dex, unlabeled, etc.)
        # chain: arbitrum, optimism, polygon_zkevm, etc. OR all
        # top_by: gas or txcount
        # days: 7, 30, 90, 180, 365
        # @TODO technically this method can be simplified because it is only used for blockspace overview
        # """
        # def get_top_contracts_by_category(self, category_type, category, chain, top_by, days):
        #         date_string = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ''
        #         if chain == 'all':
        #                 chain_string = ''
        #         else:
        #                 chain_string = f"and cl.origin_key = '{chain}'"

        #         if category_type == 'main_category':
        #                 category_string = 'bcm.main_category_key'
        #         elif category_type == 'sub_category':
        #                 category_string = 'bl.sub_category_key'
        #         else:
        #                 print('invalid category type')
        #                 raise ValueError
                
        #         category_clause = f"and lower({category_string}) = lower('{category}')" if category != 'all' else ''
                
        #         if top_by == 'gas':
        #                 top_by_string = 'gas_fees_eth'
        #         elif top_by == 'txcount':
        #                 top_by_string = 'txcount'

        #         if category == 'unlabeled':
        #                 exec_string = f'''
        #                         SELECT 
        #                                 cl.address,
        #                                 cl.origin_key,
        #                                 null as contract_name,
        #                                 null as project_name,
        #                                 null as sub_category_key,
        #                                 null as sub_category_name,
        #                                 'unlabeled' as main_category_key,
        #                                 'Unlabeled' as main_category_name,
        #                                 sum(gas_fees_eth) as gas_fees_eth,
        #                                 sum(gas_fees_usd) as gas_fees_usd,
        #                                 sum(txcount) as txcount,
        #                                 round(avg(daa)) as daa
        #                         FROM public.blockspace_fact_contract_level cl
        #                         left join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
        #                         where 
        #                                 bl.address is null
        #                                 and date < DATE_TRUNC('day', NOW())
        #                                 {date_string}
        #                                 {chain_string}
        #                         group by 1,2,3,4,5,6,7,8
        #                         order by {top_by_string} desc
        #                         limit 100
        #                 '''

        #         else:
        #                 exec_string = f'''
        #                         SELECT 
        #                                 cl.address,
        #                                 cl.origin_key,
        #                                 bl.contract_name,
        #                                 bl.project_name,
        #                                 bl.sub_category_key,
        #                                 bcm.sub_category_name,
        #                                 bcm.main_category_key,
        #                                 bcm.main_category_name,
        #                                 sum(gas_fees_eth) as gas_fees_eth,
        #                                 sum(gas_fees_usd) as gas_fees_usd,
        #                                 sum(txcount) as txcount,
        #                                 round(avg(daa)) as daa
        #                         FROM public.blockspace_fact_contract_level cl
        #                         inner join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
        #                         inner join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key) 
        #                         where 
        #                                 date < DATE_TRUNC('day', NOW())
        #                                 {date_string}
        #                                 {chain_string}
        #                                 {category_clause}
        #                         group by 1,2,3,4,5,6,7,8
        #                         order by {top_by_string} desc
        #                         limit 100
        #                 '''


        #         df = pd.read_sql(exec_string, self.engine.connect())
        #         return df
        
        """
        special function for the blockspace overview dashboard
        it returns the top 100 contracts by gas fees for the given main category
        and the top 20 contracts by gas fees for each chain in the main category
        
        """
        def get_contracts_overview(self, main_category, days):
                date_string = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ''
                if main_category.lower() != 'unlabeled':
                        main_category_string = f"and bcm.main_category_key = lower('{main_category}')" 
                        sub_main_string = """
                                bl.sub_category_key,
                                bcm.sub_category_name,
                                bcm.main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_key is null'
                        sub_main_string = """
                                'unlabeled' as sub_category_key,
                                'Unlabeled' as sub_category_name,
                                'unlabeled' as main_category_key,
                                'Unlabeled' as main_category_name,
                        """
                

                exec_string = f'''
                        with top_contracts as (
                                SELECT 
                                        cl.address,
                                        cl.origin_key,
                                        bl.contract_name,
                                        bl.project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                left join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key) 
                                where 
                                        date < DATE_TRUNC('day', NOW())
                                        {date_string}
                                        {main_category_string}
                                group by 1,2,3,4,5,6,7,8
                                order by gas_fees_eth  desc
                                ),
                                
                        top_contracts_main_category_and_origin_key as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY main_category_key, origin_key ORDER BY gas_fees_eth desc) AS r,
                                                t.*
                                        FROM
                                                top_contracts t) x
                                WHERE
                                        x.r <= 20
                                )
                                
                        select * from (select * from top_contracts order by gas_fees_eth desc limit 100) a
                        union select * from top_contracts_main_category_and_origin_key
                '''
                # print(main_category)
                # print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        

        """
        This function is used to get the top contracts by category for the landing page's top 6 contracts section. It returns the top 6 contracts by gas fees for all categories and also returns the change in the top_by metric for the given contract and time period.
        top_by: gas or txcount
        days: 1, 7, 30, 90, 180, 365
        """
        def get_top_contracts_for_all_chains_with_change(self, top_by, days, limit=6):
                if top_by == 'gas':
                        top_by_string = 'gas_fees_eth'
                elif top_by == 'txcount':
                        top_by_string = 'txcount'
                elif top_by == 'daa':
                        top_by_string = 'daa'


                exec_string = f'''
                        -- get top 6 contracts by gas fees for all chains for the given time period
                        with top_contracts as (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        bl.contract_name,
                                        bl.project_name,
                                        bl.sub_category_key,
                                        bcm.sub_category_name,
                                        bcm.main_category_key,
                                        bcm.main_category_name,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key
                                join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key)
                                where
                                        date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                group by 1,2,3,4,5,6,7,8
                                order by {top_by_string} desc
                                limit {limit}
                        ),
                        -- get the change in the gas_fees_eth, gas_fees_usd, txcount, and daa for the given contracts for the time period before the selected time period
                        prev as (
                                SELECT
                                        cl.address,
                                        cl.origin_key,
                                        bl.contract_name,
                                        bl.project_name,
                                        bl.sub_category_key,
                                        bcm.sub_category_name,
                                        bcm.main_category_key,
                                        bcm.main_category_name,
                                        sum(cl.gas_fees_eth) as gas_fees_eth,
                                        sum(cl.gas_fees_usd) as gas_fees_usd,
                                        sum(cl.txcount) as txcount,
                                        round(avg(cl.daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                inner join top_contracts tc on tc.address = cl.address and tc.origin_key = cl.origin_key 
                                join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key
                                join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key)
                                where
                                        date < DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days*2} days')
                                group by 1,2,3,4,5,6,7,8
                        )
                        -- join the two tables together to get the change in the top_by metric for the given contracts
                        select
                                tc.address,
                                tc.origin_key,
                                tc.contract_name,
                                tc.project_name,
                                tc.sub_category_key,
                                tc.sub_category_name,
                                tc.main_category_key,
                                tc.main_category_name,
                                tc.gas_fees_eth,
                                tc.gas_fees_usd,
                                tc.txcount,
                                tc.daa,
                                tc.gas_fees_eth - p.gas_fees_eth as gas_fees_eth_change,
                                tc.gas_fees_usd - p.gas_fees_usd as gas_fees_usd_change,
                                tc.txcount - p.txcount as txcount_change,
                                tc.daa - p.daa as daa_change,
                                p.gas_fees_eth as prev_gas_fees_eth,
                                p.gas_fees_usd as prev_gas_fees_usd,
                                p.txcount as prev_txcount,
                                p.daa as prev_daa,
                                ROUND(((tc.gas_fees_eth - p.gas_fees_eth) / p.gas_fees_eth)::numeric, 4) as gas_fees_eth_change_percent,
                                ROUND(((tc.gas_fees_usd - p.gas_fees_usd) / p.gas_fees_usd)::numeric, 4) as gas_fees_usd_change_percent,
                                ROUND(((tc.txcount - p.txcount) / p.txcount)::numeric, 4) as txcount_change_percent,
                                ROUND(((tc.daa - p.daa) / p.daa)::numeric, 4) as daa_change_percent
                        from top_contracts tc
                        left join prev p on tc.address = p.address
                '''

                df = pd.read_sql(exec_string, self.engine.connect())

                return df
                
        
        """
        special function for the blockspace category comparison dashboard
        it returns the top 50 contracts by gas fees for the given main category
        and the top 10 contracts by gas fees for each sub category in the main category
        
        """
        def get_contracts_category_comparison(self, main_category, days, origin_keys:list):
                date_string = f"and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')" if days != 'max' else ''
                if main_category.lower() != 'unlabeled':
                        main_category_string = f"and bcm.main_category_key = lower('{main_category}')" 
                        sub_main_string = """
                                bl.sub_category_key,
                                bcm.sub_category_name,
                                bcm.main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_key is null'
                        sub_main_string = """
                                'unlabeled' as sub_category_key,
                                'Unlabeled' as sub_category_name,
                                'unlabeled' as main_category_key,
                                'Unlabeled' as main_category_name,
                        """
                

                exec_string = f'''
                        with top_contracts as (
                                SELECT 
                                        cl.address,
                                        cl.origin_key,
                                        bl.contract_name,
                                        bl.project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                left join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key) 
                                where 
                                        date < DATE_TRUNC('day', NOW())
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                        {date_string}
                                        {main_category_string}
                                group by 1,2,3,4,5,6,7,8
                                order by gas_fees_eth desc
                                ),
                                
                        top_contracts_sub_category_and_origin_key as (
                                SELECT
                                        address,origin_key,contract_name,project_name,sub_category_key,sub_category_name,main_category_key,main_category_name,gas_fees_eth,gas_fees_usd,txcount,daa
                                FROM (
                                        SELECT
                                                ROW_NUMBER() OVER (PARTITION BY sub_category_key, origin_key ORDER BY gas_fees_eth desc) AS r,
                                                t.*
                                        FROM
                                                top_contracts t) x
                                WHERE
                                x.r <= 20
                                )
                                
                        select * from (select * from top_contracts order by gas_fees_eth desc limit 50) a
                        union select * from top_contracts_sub_category_and_origin_key
                '''
                # print(main_category)
                # print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_imx(self, days):
                exec_string = f'''
                         with 
                                cte_imx_deposits as (
                                        select 
                                                date_trunc('day', "timestamp") as day, 
                                                'bridge' as sub_category_key,
                                                Count(*) as txcount                    
                                        from imx_deposits
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days} days'
                                        group by 1
                                ),	
                                cte_imx_mints as (
                                                select 
                                                        date_trunc('day', "timestamp") as day, 
                                                        'erc721' as sub_category_key,
                                                        Count(*) as txcount 
                                                from imx_mints
                                                WHERE timestamp < date_trunc('day', now())
                                                        AND timestamp >= date_trunc('day',now()) - interval '{days}  days'
                                                group by 1
                                        ),    
                                cte_imx_trades as (
                                        select 
                                                date_trunc('day', "timestamp") as day, 
                                                'nft_marketplace' as sub_category_key,
                                                Count(*) as txcount                        
                                        from imx_trades
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days}  days'
                                        group by 1
                                ),    
                                cte_imx_transfers_erc20 as (
                                        select 
                                                date_trunc('day', "timestamp") as day,
                                                'erc20' as sub_category_key,
                                                Count(*) as txcount
                                        from imx_transfers
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days}  days'
                                                and token_type = 'ERC20'
                                        group by 1
                                ),
                                cte_imx_transfers_erc721 as (
                                        select 
                                                date_trunc('day', "timestamp") as day,
                                                'erc721' as sub_category_key,
                                                Count(*) as txcount
                                        from imx_transfers
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days} days'
                                                and token_type = 'ERC721'
                                        group by 1
                                ),
                                cte_imx_transfers_eth as (
                                        select 
                                                date_trunc('day', "timestamp") as day,
                                                'native_transfer' as sub_category_key,
                                                Count(*) as txcount
                                        from imx_transfers
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days} days'
                                                and token_type = 'ETH'
                                        group by 1
                                ),
                                cte_imx_withdrawals as (
                                        select 
                                        date_trunc('day', "timestamp") as day, 
                                        'bridge' as sub_category_key,
                                        Count(*) as txcount     
                                        from imx_withdrawals  
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days} days'
                                        group by 1
                                ),
                                unioned as (
                                        select * from cte_imx_deposits
                                        union all
                                        select * from cte_imx_mints
                                        union all
                                        select * from cte_imx_withdrawals
                                        union all
                                        select * from cte_imx_trades
                                        union all
                                        select * from cte_imx_transfers_erc20 
                                        union all
                                        select * from cte_imx_transfers_erc721 
                                        union all
                                        select * from cte_imx_transfers_eth
                                )
                                select 
                                        day as date, 
                                        sub_category_key,
                                        'imx' as origin_key,
                                        SUM(txcount) as txcount 
                                from unioned 
                                group by 1,2
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_unlabelled_contracts(self, number_of_contracts, days):
                exec_string = f'''
                        WITH ranked_contracts AS (
                                SELECT 
                                        cl.address, 
                                        SUM(gas_fees_eth) AS gas_eth, 
                                        SUM(txcount) AS txcount, 
                                        SUM(daa) AS daa, 
                                        cl.origin_key, 
                                        ROW_NUMBER() OVER (PARTITION BY cl.origin_key ORDER BY SUM(gas_fees_eth) DESC) AS row_num 
                                FROM public.blockspace_fact_contract_level cl 
                                LEFT JOIN blockspace_labels bl ON cl.address = bl.address AND cl.origin_key = bl.origin_key 
                                WHERE bl.address IS NULL 
                                AND cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                AND cl.address != decode('4E6F6E65', 'hex') 
                                GROUP BY cl.address, cl.origin_key
                        )  
                        SELECT 
                                address, 
                                gas_eth, 
                                txcount, 
                                daa, 
                                origin_key 
                        FROM ranked_contracts 
                        WHERE row_num <= {number_of_contracts} 
                        ORDER BY origin_key, row_num
    
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
