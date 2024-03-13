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
        def get_last_price_eth(self, origin_key:str):
                try:
                        query = f"SELECT value FROM fact_kpis WHERE origin_key = '{origin_key}' AND metric_key = 'price_eth' ORDER BY date DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in ETH for {origin_key}.")
                        print(e)
                        return None
                
        def get_last_price_usd(self, origin_key:str):
                try:
                        query = f"SELECT value FROM fact_kpis WHERE origin_key = '{origin_key}' AND metric_key = 'price_usd' ORDER BY date DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in USD for {origin_key}.")
                        print(e)
                        return None
                
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
        
        def get_max_block(self, table_name:str, date:str=None):
                if date is None:
                        exec_string = f"SELECT MAX(block_number) as val FROM {table_name};"
                else:
                        exec_string = f"SELECT MAX(block_number) as val FROM {table_name} WHERE date_trunc('day', block_timestamp) = '{date}';"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        val = row['val']
                
                if val == None:
                        return 0
                else:
                        return val
                
        def get_min_block(self, table_name:str, date:str=None):
                if date is None:
                        exec_string = f"SELECT MIN(block_number) as val FROM {table_name};"
                else:
                        exec_string = f"SELECT MIN(block_number) as val FROM {table_name} WHERE date_trunc('day', block_timestamp) = '{date}';"

                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                for row in result:
                        val = row['val']
                
                if val == None:
                        return 0
                else:
                        return val
                
        def get_profit_in_eth(self, days, exclude_chains, origin_keys = None):               
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                exec_string = f'''
                        with tmp as (
                                SELECT 
                                        date,
                                        origin_key,
                                        SUM(CASE WHEN metric_key = 'rent_paid_eth' THEN value END) AS rent_paid_eth,
                                        SUM(CASE WHEN metric_key = 'fees_paid_eth' THEN value END) AS fees_paid_eth
                                FROM fact_kpis tkd
                                WHERE metric_key = 'rent_paid_eth' or metric_key = 'fees_paid_eth'
                                        AND origin_key not in ('{"','".join(exclude_chains)}')
                                        {ok_string}
                                        AND date >= date_trunc('day',now()) - interval '{days} days'
                                        AND date < date_trunc('day', now())
                                GROUP BY 1,2
                        )

                        SELECT
                                date, 
                                origin_key,
                                'profit_eth' as metric_key,
                                fees_paid_eth - rent_paid_eth as value 
                        FROM tmp
                        WHERE rent_paid_eth > 0 and fees_paid_eth > 0
                        ORDER BY 1 desc
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_fdv_in_usd(self, days, origin_keys = None):               
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                exec_string = f'''
                        with tmp as (
                                SELECT 
                                        date,
                                        origin_key,
                                        SUM(CASE WHEN metric_key = 'price_usd' THEN value END) AS price_usd,
                                        SUM(CASE WHEN metric_key = 'total_supply' THEN value END) AS total_supply
                                FROM fact_kpis tkd
                                WHERE metric_key = 'price_usd' or metric_key = 'total_supply'
                                        {ok_string}
                                        AND date >= date_trunc('day',now()) - interval '{days} days'
                                        AND date < date_trunc('day', now())
                                GROUP BY 1,2
                        )

                        SELECT
                                date, 
                                origin_key,
                                'fdv_usd' as metric_key,
                                price_usd * total_supply as value 
                        FROM tmp
                        WHERE price_usd > 0 and total_supply > 0
                        ORDER BY 1 desc
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_eth(self, metric_keys, days, origin_keys = None): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load eth values for : {mk_string} and {origin_keys}")
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
                                        WHEN 'fdv_usd' THEN 'fdv_eth'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value / p.value as value
                        FROM fact_kpis tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
                                AND tkd.date < date_trunc('day', NOW()) 
                                AND tkd.date >= date_trunc('day',now()) - interval '{days} days'
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_values_in_usd(self, metric_keys, days, origin_keys = None): ## also make sure to add new metrics in adapter_sql
                mk_string = "'" + "', '".join(metric_keys) + "'"

                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"

                print(f"load usd values for : {mk_string} and {origin_keys}")
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                Case tkd.metric_key 
                                        WHEN 'rent_paid_eth' THEN 'rent_paid_usd'
                                        WHEN 'fees_paid_eth' THEN 'fees_paid_usd'
                                        WHEN 'profit_eth' THEN 'profit_usd'
                                        WHEN 'tvl_eth' THEN 'tvl'
                                        WHEN 'stables_mcap_eth' THEN 'stables_mcap' 
                                        WHEN 'txcosts_median_eth' THEN 'txcosts_median_usd'
                                        ELSE 'error'
                                END AS metric_key, 
                                tkd.origin_key,
                                tkd."date", 
                                tkd.value * p.value as value
                        FROM fact_kpis tkd
                        LEFT JOIN eth_price p on tkd."date" = p."date"
                        WHERE tkd.metric_key in ({mk_string})
                                {ok_string}
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
        
        ## Unique sender and addresses
        def get_unique_addresses(self, chain:str, days:int):
                if chain == 'imx':
                        exec_string = f'''
                                with union_all as (
                                        SELECT 
                                                DATE_TRUNC('day', "timestamp") AS day
                                                , "user" as address
                                        FROM imx_deposits id 
                                        WHERE "timestamp" < DATE_TRUNC('day', NOW())
                                                AND "timestamp" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        
                                        UNION ALL
                                        
                                        SELECT 
                                                date_trunc('day', "timestamp") as day 
                                                , "sender" as address
                                        FROM imx_withdrawals  
                                        WHERE "timestamp" < date_trunc('day', now())
                                                AND "timestamp" >= date_trunc('day',now() - INTERVAL '{days} days')

                                        UNION ALL 
                                        
                                        SELECT 
                                                date_trunc('day', "updated_timestamp") as day 
                                                , "user" as address

                                        FROM imx_orders   
                                        WHERE updated_timestamp < date_trunc('day', now())
                                                AND updated_timestamp >= date_trunc('day',now() - INTERVAL '{days} days')
                                                
                                        UNION ALL
                                        
                                        SELECT 
                                                date_trunc('day', "timestamp") as day
                                                , "user" as address
                                        FROM imx_transfers
                                        WHERE "timestamp" < date_trunc('day', now())
                                                AND "timestamp" >= date_trunc('day',now() - INTERVAL '{days} days')
                                )

                                SELECT
                                        day as date,
                                        address,
                                        '{chain}' as origin_key,
                                        count(*) as txcount
                                FROM union_all
                                GROUP BY 1,2,3
                        '''
                else:
                        exec_string = f'''
                                SELECT 
                                        date_trunc('day', block_timestamp) as date,
                                        from_address as address,
                                        '{chain}' as origin_key,
                                        count(*) as txcount
                                FROM {chain}_tx
                                WHERE block_timestamp < DATE_TRUNC('day', NOW())
                                        AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                GROUP BY 1,2,3
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                df = df.dropna(subset=['address'])
                return df


        ## Blockspace queries
        # This function is used to get aggregate the blockspace data on contract level for a specific chain. The data will be loaded into fact_contract_level table
        # it only aggregates transactions that are NOT native transfers, system transactionsm contract creations, or inscriptions. These are aggregated separately and output is stored directly in the fact_sub_category_level table
        def get_blockspace_contracts(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
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
                                and not (from_address = to_address and empty_input = false) -- filter out inscriptions (step 1: from_address = to_address + empty_input = false)
                                and not (to_address in (select distinct address from inscription_addresses) and empty_input = false) -- filter out inscriptions (step 2: to_address in inscription_addresses + empty_input = false)
                        group by 1,2
                        having count(*) > 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to get the native_transfer daily aggregate per chain. The data will be loaded into fact_sub_category_level table        
        def get_blockspace_native_transfers(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
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
        
         # This function is used to get the inscriptions per chain. The data will be loaded into fact_sub_category_level table        
        def get_blockspace_inscriptions(self, chain, days):
                ## Mantle and Metis stores fees in own token: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
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
                                'inscriptions' as sub_category_key,
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
                                and tx_fee > 0
                                and (
                                        (from_address = to_address and empty_input = false)
                                        or 
                                        (to_address in (select distinct address from inscription_addresses) and empty_input = false)
                                )
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to get the contract_deployment daily aggregate per chain. The data will be loaded into fact_sub_category_level table
        def get_blockspace_contract_deplyments(self, chain, days):
                ## Mantle and Metis store fees in own token: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
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
                        filter_string = "and (to_address = '' or to_address is null)"

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
                #print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to get the total blockspace fees per day for a specific chain. The data will be loaded into fact_sub_category_level table.
        def get_blockspace_total(self, chain, days):
                ## Mantle and Metis store fees in own tokens: hence different logic for gas_fees_eth and gas_fees_usd
                if chain in ['mantle', 'metis']:
                        additional_cte = f"""
                                , token_price AS (
                                        SELECT "date", value
                                        FROM public.fact_kpis
                                        WHERE origin_key = '{chain}' and metric_key = 'price_usd'
                                )
                        """
                        tx_fee_eth_string = 'tx_fee * mp.value / p.value'
                        tx_fee_usd_string = 'tx_fee * mp.value'                        
                        additional_join = """LEFT JOIN token_price mp on date_trunc('day', tx.block_timestamp) = mp."date" """
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
        
        # This function is used to aggregate the blockspace data from contract_level, map it to categories, and then it will be loaded into the sub_category level table. The data will be loaded into fact_sub_category_level table
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
        
        # This function is used to calculate the unlabeled blockspace data for a specific chain. The data will be loaded into fact_sub_category_level table
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
        
        
        """
        function for the blockspace overview json and the single chain blockspace jsons
        it returns the top 100 contracts by gas fees for the given main category
        and the top 20 contracts by gas fees for each chain in the main category
        
        """
        def get_contracts_overview(self, main_category, days, origin_keys, contract_limit=100):
                
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
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
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
                                
                        select * from (select * from top_contracts order by gas_fees_eth desc limit {contract_limit}) a
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
        def get_top_contracts_for_all_chains_with_change(self, top_by, days, origin_keys, limit=6):
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
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
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
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
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
                        left join prev p on tc.address = p.address and tc.origin_key = p.origin_key
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
                                AND NOT EXISTS (
                                        SELECT 1
                                        FROM public.inscription_addresses ia
                                        WHERE ia.address = cl.address
                                )
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
        
        def get_total_supply_blocks(self, origin_key, days):
                exec_string = f'''
                        SELECT 
                                DATE(block_timestamp) AS date,
                                MAX(block_number) AS block_number
                        FROM public.{origin_key}_tx
                        WHERE DATE(block_timestamp) BETWEEN (CURRENT_DATE - INTERVAL '{days+1} days') AND (CURRENT_DATE - INTERVAL '1 day')
                        GROUP BY DATE(block_timestamp);
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
