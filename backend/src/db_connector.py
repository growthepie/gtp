from datetime import datetime
from pangres import upsert
from sqlalchemy import text
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
            self.uri = f"postgresql://{db_user}:{db_passwd}@{db_host}/{db_name}"
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
                                upsert(con=self.engine, df=df, table_name=table_name, if_row_exists=if_exists, create_table=False)
                        return df.shape[0]
                
# ------------------------- additional db functions --------------------------------
        def refresh_materialized_view(self, view_name:str):
                exec_string = f"REFRESH MATERIALIZED VIEW {view_name};"
                with self.engine.connect() as connection:
                        connection.execute(exec_string)
                print(f"Materialized view {view_name} refreshed.")
        
        def get_last_price_eth(self, origin_key:str, granularity:str='daily'):
                if granularity == 'daily':
                        table_name = 'fact_kpis'
                        granularity_filter = ''
                        order_by_col = 'date'
                elif granularity == 'hourly':
                        table_name = 'fact_kpis_granular'
                        granularity_filter = "AND granularity = 'hourly'"
                        order_by_col = 'timestamp'

                try:
                        query = f"SELECT value FROM {table_name} WHERE origin_key = '{origin_key}' AND metric_key = 'price_eth' {granularity_filter} ORDER BY {order_by_col} DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in ETH for {origin_key}.")
                        print(e)
                        return None
                
        def get_last_price_usd(self, origin_key:str, granularity:str='daily'):
                if granularity == 'daily':
                        table_name = 'fact_kpis'
                        granularity_filter = ''
                        order_by_col = 'date'
                elif granularity == 'hourly':
                        table_name = 'fact_kpis_granular'
                        granularity_filter = "AND granularity = 'hourly'"
                        order_by_col = 'timestamp'

                try:
                        query = f"SELECT value FROM {table_name} WHERE origin_key = '{origin_key}' AND metric_key = 'price_usd' {granularity_filter} ORDER BY {order_by_col} DESC LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                latest_price = result.scalar()
                                return latest_price
                except Exception as e:
                        print(f"Error retrieving the latest price in USD for {origin_key}.")
                        print(e)
                        return None
                
        def get_main_config_dict(self):
                exec_string = "SELECT * FROM sys_chains"

                df = pd.read_sql(exec_string, self.engine.connect())

                ## break up all columns with dictionaries into separate columns but keep the original column name as a prefix
                for column in ['api', 'aliases', 'metadata', 'socials', 'runs', 'backfiller', 'cross_check', 'circulating_supply']:
                        df = pd.concat([df.drop([column], axis=1), df[column].apply(pd.Series, dtype='object').add_prefix(column + '_')], axis=1)

                df = df.where(pd.notnull(df), None)
                df['backfiller_batch_size'] = df['backfiller_batch_size'].fillna(0). astype(int)

                main_config = df.to_dict(orient='records')
                return main_config
                
        def get_stage(self, origin_key:str):
                try:
                        query = f"SELECT l2beat_stage FROM sys_chains WHERE origin_key = '{origin_key}' LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                stage = result.scalar()
                                return stage
                except Exception as e:
                        print(f"Error retrieving the stage for {origin_key}.")
                        print(e)
                        return None
                
        def get_chain_info(self, origin_key:str, column:str):
                try:
                        query = f"SELECT {column} FROM sys_chains WHERE origin_key = '{origin_key}' LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                value = result.scalar()
                                return value
                except Exception as e:
                        print(f"Error retrieving {column} for {origin_key}.")
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
                

        """
        The get_economics_in_eth function is used to get the economics data on chain level in ETH. The following metrics are calculated:
        - blob_size_bytes: total blob size in bytes (currently sum of celestia and ethereum blob size)
        - costs_blobs_eth: costs for storing blobs in ETH (sum of celestia and ethereum blobs)
        - costs_da_eth: costs for data availability in ETH (sum of celestia and ethereum blobs and l1 data availability)
        - costs_l1_eth: costs for ethereuem excl blobs (sum of l1 data availability and l1 settlement)
        - rent_paid_eth: total amount of fees that is paid to Ethereum (blobs and l1 data availability and l1 settlement)
        - costs_total_eth: total costs in ETH (sum of all costs of a chain)
        - profit_eth: profit in ETH (fees paid - costs_total_eth)
        """
        def get_economics_in_eth(self, days, exclude_chains, origin_keys = None, incl_profit = True):               
                if exclude_chains is None or len(exclude_chains) == 0:
                        exclude_string = ''
                else:
                        exclude_string = "AND tkd.origin_key not in ('" + "', '".join(exclude_chains) + "')"
                
                if origin_keys is None or len(origin_keys) == 0:
                        ok_string = ''
                else:
                        ok_string = "AND tkd.origin_key in ('" + "', '".join(origin_keys) + "')"
                
                if incl_profit:
                        profit_string = ",SUM(CASE WHEN metric_key = 'fees_paid_eth' THEN value END) - SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'l1_data_availability_eth', 'l1_settlement_eth') THEN value END) AS profit_eth"
                else:
                        profit_string = ''

                exec_string = f'''
                        SELECT 
                                date
                                ,origin_key
                                ,SUM(CASE WHEN metric_key in ('ethereum_blob_size_bytes', 'celestia_blob_size_bytes') THEN value END) AS blob_size_bytes

                                ,SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth') THEN value END) AS costs_blobs_eth
                                ,SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'l1_data_availability_eth') THEN value END) AS costs_da_eth
                                ,SUM(CASE WHEN metric_key in ('l1_data_availability_eth', 'l1_settlement_eth') THEN value END) AS costs_l1_eth
                                ,SUM(CASE WHEN metric_key in ('l1_data_availability_eth', 'l1_settlement_eth', 'ethereum_blobs_eth') THEN value END) AS rent_paid_eth

                                ,SUM(CASE WHEN metric_key in ('ethereum_blobs_eth', 'celestia_blobs_eth', 'l1_data_availability_eth', 'l1_settlement_eth') THEN value END) AS costs_total_eth
                                {profit_string}
                        FROM fact_kpis tkd
                        WHERE metric_key in ('ethereum_blob_size_bytes', 'celestia_blob_size_bytes', 'celestia_blobs_eth', 'ethereum_blobs_eth', 'l1_data_availability_eth', 'l1_settlement_eth', 'fees_paid_eth')
                                {exclude_string}
                                {ok_string}
                                AND date >= date_trunc('day',now()) - interval '{days} days'
                                AND date < date_trunc('day', now())
                        GROUP BY 1,2
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
                                        WHEN 'l1_data_availability_eth' THEN 'l1_data_availability_usd'
                                        WHEN 'l1_settlement_eth' THEN 'l1_settlement_usd'
                                        WHEN 'ethereum_blobs_eth' THEN 'ethereum_blobs_usd'
                                        WHEN 'celestia_blobs_eth' THEN 'celestia_blobs_usd'
                                        WHEN 'costs_blobs_eth' THEN 'costs_blobs_usd'
                                        WHEN 'costs_l1_eth' THEN 'costs_l1_usd'
                                        WHEN 'costs_da_eth' THEN 'costs_da_usd'
                                        WHEN 'costs_total_eth' THEN 'costs_total_usd'
                                        WHEN 'total_blobs_eth' THEN 'total_blobs_usd'
                                        WHEN 'fees_paid_eth' THEN 'fees_paid_usd'
                                        WHEN 'profit_eth' THEN 'profit_usd'
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
        def aggregate_unique_addresses(self, chain:str, days:int, days_end:int=None):

                if days_end is None:
                        days_end_string = "DATE_TRUNC('day', NOW())"
                else:
                        if days_end > days:
                                raise ValueError("days_end must be smaller than days")
                        days_end_string = f"DATE_TRUNC('day', NOW() - INTERVAL '{days_end} days')"

                if chain == 'imx':
                        exec_string = f'''
                                with union_all as (
                                        SELECT 
                                                DATE_TRUNC('day', "timestamp") AS day
                                                , "user" as address
                                        FROM imx_deposits id 
                                        WHERE "timestamp" < {days_end_string}
                                                AND "timestamp" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        
                                        UNION ALL
                                        
                                        SELECT 
                                                date_trunc('day', "timestamp") as day 
                                                , "sender" as address
                                        FROM imx_withdrawals  
                                        WHERE "timestamp" < {days_end_string}
                                                AND "timestamp" >= date_trunc('day',now() - INTERVAL '{days} days')

                                        UNION ALL 
                                        
                                        SELECT 
                                                date_trunc('day', "updated_timestamp") as day 
                                                , "user" as address

                                        FROM imx_orders   
                                        WHERE updated_timestamp < {days_end_string}
                                                AND updated_timestamp >= date_trunc('day',now() - INTERVAL '{days} days')
                                                
                                        UNION ALL
                                        
                                        SELECT 
                                                date_trunc('day', "timestamp") as day
                                                , "user" as address
                                        FROM imx_transfers
                                        WHERE "timestamp" < {days_end_string}
                                                AND "timestamp" >= date_trunc('day',now() - INTERVAL '{days} days')
                                )

                                INSERT INTO fact_active_addresses (address, date, origin_key, txcount)
                                        SELECT                                                
                                                address,
                                                day as date,
                                                '{chain}' as origin_key,
                                                count(*) as txcount
                                        FROM union_all
                                        GROUP BY 1,2,3
                                ON CONFLICT (origin_key, date, address)
                                DO UPDATE SET txcount = EXCLUDED.txcount;
                        '''
                else:
                        exec_string = f'''
                                INSERT INTO fact_active_addresses (address, date, origin_key, txcount)
                                        SELECT 
                                                from_address as address,
                                                date_trunc('day', block_timestamp) as date,                                                
                                                '{chain}' as origin_key,
                                                count(*) as txcount
                                        FROM {chain}_tx
                                        WHERE block_timestamp < {days_end_string}
                                                AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                                and from_address is not null
                                        GROUP BY 1,2,3
                                ON CONFLICT (origin_key, date, address)
                                DO UPDATE SET txcount = EXCLUDED.txcount;
                        '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"Unique addresses for {chain} and {days} days aggregated and loaded into fact_active_addresses.")

                
        
        ## This method aggregates on top of fact_active_addresses and stores memory efficient hll hashes in fact_active_addresses_hll
        def aggregate_unique_addresses_hll(self, chain:str, days:int):   
                ## in starknets case go straight to the source (raw tx data) because we don't add it to fact_active_addresses  
                if chain in ['starknet']:    
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_hll (origin_key, date, hll_addresses)
                                        SELECT 
                                                '{chain}' as origin_key,
                                                date_trunc('day', block_timestamp) as date,
                                                hll_add_agg(hll_hash_text(from_address), 17,5,-1,1)        
                                        FROM {chain}_tx
                                        WHERE block_timestamp < DATE_TRUNC('day', NOW())
                                                AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        GROUP BY 1,2
                                ON CONFLICT (origin_key, date)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                else:
                        exec_string = f'''
                                INSERT INTO fact_active_addresses_hll (origin_key, date, hll_addresses) 
                                        select 
                                                origin_key, 
                                                date, 
                                                hll_add_agg(hll_hash_bytea(address), 17,5,-1,1)
                                        from fact_active_addresses 
                                        where origin_key = '{chain}' 
                                                and date < DATE_TRUNC('day', NOW())
                                                AND date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        group by 1,2
                                ON CONFLICT (origin_key, date)
                                DO UPDATE SET hll_addresses = EXCLUDED.hll_addresses;
                        '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"HLL hashes for {chain} and {days} days loaded into fact_active_addresses_hll.")

        ## This method aggregates address for the fact_unique_addresses table and determines when an address was first seen
        def aggregate_addresses_first_seen_global(self, days:int):   
                
                exec_string = f'''
                        INSERT INTO fact_unique_addresses (address, first_seen_global)
                                WITH not_added_yet AS (
                                        SELECT DISTINCT faa.address
                                        FROM fact_active_addresses faa
                                        WHERE faa."date" < current_date 
                                        AND faa."date" >= current_date - INTERVAL '{days}' day 
                                        AND NOT EXISTS (
                                                SELECT 1
                                                FROM fact_unique_addresses fua
                                                WHERE fua.address = faa.address
                                        )
                                )
                                
                                SELECT 
                                        nay.address,
                                        MIN(faa."date") AS first_seen_global
                                FROM fact_active_addresses faa
                                INNER JOIN not_added_yet nay ON faa.address = nay.address
                                GROUP BY nay.address
                        ON CONFLICT (address)
                        DO UPDATE SET first_seen_global = EXCLUDED.first_seen_global
                                WHERE fact_unique_addresses.first_seen_global > EXCLUDED.first_seen_global;
                '''

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"Unique addresses with first seen for {days} days loaded into fact_unique_addresses.")
               

        def get_total_supply_blocks(self, origin_key, days):
                exec_string = f'''
                        SELECT 
                                DATE(block_timestamp) AS date,
                                MAX(block_number) AS block_number
                        FROM public.{origin_key}_tx
                        WHERE block_timestamp BETWEEN (CURRENT_DATE - INTERVAL '{days+1} days') AND (CURRENT_DATE)
                        GROUP BY 1;
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
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
                        
                        INSERT INTO blockspace_fact_contract_level (address, date, gas_fees_eth, gas_fees_usd, txcount, daa, origin_key, success_rate, median_tx_fee)
                                select
                                        to_address as address,
                                        date_trunc('day', block_timestamp) as date,
                                        sum({tx_fee_eth_string}) as gas_fees_eth,
                                        sum({tx_fee_usd_string}) as gas_fees_usd,
                                        count(*) as txcount,
                                        count(distinct from_address) as daa,
                                        '{chain}' as origin_key,
                                        sum(status)*1.0/count(*) as success_rate,
                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
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
                        ON CONFLICT (origin_key, date, address)
                        DO UPDATE SET
                                txcount = EXCLUDED.txcount,
                                daa = EXCLUDED.daa,
                                gas_fees_eth = EXCLUDED.gas_fees_eth,
                                gas_fees_usd = EXCLUDED.gas_fees_usd,
                                success_rate = EXCLUDED.success_rate,
                                median_tx_fee = EXCLUDED.median_tx_fee;
                '''
                # df = pd.read_sql(exec_string, self.engine.connect())
                # return df

                with self.engine.connect() as connection:
                        with connection.begin():
                                connection.execute(text(exec_string))
                print(f"...data inserted successfully for {chain} and {days}.")

        
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
                                'native_transfer' as category_id,
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
                                'inscriptions' as category_id,
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
                                'contract_deployment' as category_id,
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
                                'total_usage' as category_id,
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
        

        def get_blockspace_total_imx(self, days):
                exec_string = f'''
                        SELECT 
                                "date", 
                                'total_usage' as category_id,
                                'imx' as origin_key,
                                sum(gas_fees_eth) as gas_fees_eth,
                                sum(gas_fees_usd) as gas_fees_usd, 
                                sum(txcount) as txcount,
                                sum(daa) as daa
                        FROM public.blockspace_fact_category_level
                        where origin_key = 'imx'
                                and category_id not in ('total_usage')
                                and "date" < DATE_TRUNC('day', NOW())
                                and "date" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                        group by 1
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        # This function is used to aggregate the blockspace data from contract_level, map it to categories, and then it will be loaded into the sub_category level table. The data will be loaded into fact_sub_category_level table
        def get_blockspace_sub_categories(self, chain, days):
                exec_string = f'''
                        SELECT 
                                lower(bl.usage_category) as category_id,
                                '{chain}' as origin_key,
                                date,
                                sum(gas_fees_eth) as gas_fees_eth,
                                sum(gas_fees_usd) as gas_fees_usd,
                                sum(txcount) as txcount,
                                sum(daa) as daa
                        FROM public.blockspace_fact_contract_level cl
                        inner join vw_oli_labels_materialized bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                        where date < DATE_TRUNC('day', NOW())
                                and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and cl.origin_key = '{chain}'
                                and bl.usage_category is not null 
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
                                FROM public.blockspace_fact_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and category_id <> 'unlabeled' and category_id <> 'total_usage'
                                group by 1
                        ),
                        total_usage as (
                                SELECT 
                                        date,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount
                                FROM public.blockspace_fact_category_level
                                where date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and origin_key = '{chain}'
                                        and category_id = 'total_usage'
                                group by 1
                        )

                        select 
                                t.date,
                                'unlabeled' as category_id,
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
                        main_category_string = f"and bcm.main_category_id = lower('{main_category}')" 
                        sub_main_string = """
                                bl.usage_category as sub_category_key,
                                bcm.category_name as sub_category_name,
                                bcm.main_category_id as main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_id is null'
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
                                        bl.name as contract_name,
                                        oss.display_name as project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_labels_materialized bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id) 
                                left join oli_oss_directory oss on bl.owner_project = oss.name
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
                                        FROM top_contracts t) x
                                WHERE x.r <= 20
                                )
                                
                        select * from (select * from top_contracts order by gas_fees_eth desc limit {contract_limit}) a
                        union select * from top_contracts_main_category_and_origin_key
                '''
                # print(main_category)
                # print(exec_string)
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        

        """
        This function is used to get the top contracts by category for the landing page's top 6 contracts section and the single chain hottest contract.
        It returns the top 6 contracts by gas fees for all categories and also returns the change in the top_by metric for the given contract and time period.
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
                                        bl.name as contract_name,
                                        oss.display_name as project_name,
                                        bl.usage_category as sub_category_key,
                                        bcm.category_name as sub_category_name,
                                        bcm.main_category_id as main_category_key,
                                        bcm.main_category_name,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_labels_materialized bl on cl.address = bl.address and cl.origin_key = bl.origin_key
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id)
                                left join oli_oss_directory oss on bl.owner_project = oss.name
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
                                        sum(cl.gas_fees_eth) as gas_fees_eth,
                                        sum(cl.gas_fees_usd) as gas_fees_usd,
                                        sum(cl.txcount) as txcount,
                                        round(avg(cl.daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                inner join top_contracts tc on tc.address = cl.address and tc.origin_key = cl.origin_key 
                                where
                                        date < DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days*2} days')
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                group by 1,2
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
                        main_category_string = f"and bcm.main_category_id = lower('{main_category}')" 
                        sub_main_string = """
                                bl.usage_category as sub_category_key,
                                bcm.category_name as sub_category_name,
                                bcm.main_category_id as main_category_key,
                                bcm.main_category_name,
                        """
                else:
                        main_category_string = 'and bcm.main_category_id is null'
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
                                        bl.name as contract_name,
                                        oss.display_name as project_name,
                                        {sub_main_string}
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join vw_oli_labels_materialized bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                left join vw_oli_category_mapping bcm on lower(bl.usage_category) = lower(bcm.category_id) 
                                left join oli_oss_directory oss on bl.owner_project = oss.name
                                where 
                                        date < DATE_TRUNC('day', NOW())
                                        and cl.origin_key IN ('{"','".join(origin_keys)}')
                                        {date_string}
                                        {main_category_string}
                                group by 1,2,3,4,5,6,7,8
                                order by gas_fees_eth desc
                                ),
                                
                        top_contracts_category_and_origin_key as (
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
                        union select * from top_contracts_category_and_origin_key
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
                                                'bridge' as category_id,
                                                Count(*) as txcount                    
                                        from imx_deposits
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days} days'
                                        group by 1
                                ),	
                                cte_imx_mints as (
                                                select 
                                                        date_trunc('day', "timestamp") as day, 
                                                        'non_fungible_tokens' as category_id,
                                                        Count(*) as txcount 
                                                from imx_mints
                                                WHERE timestamp < date_trunc('day', now())
                                                        AND timestamp >= date_trunc('day',now()) - interval '{days}  days'
                                                group by 1
                                        ),    
                                cte_imx_trades as (
                                        select 
                                                date_trunc('day', "timestamp") as day, 
                                                'nft_marketplace' as category_id,
                                                Count(*) as txcount                        
                                        from imx_trades
                                        WHERE timestamp < date_trunc('day', now())
                                                AND timestamp >= date_trunc('day',now()) - interval '{days}  days'
                                        group by 1
                                ),    
                                cte_imx_transfers_erc20 as (
                                        select 
                                                date_trunc('day', "timestamp") as day,
                                                'fungible_tokens' as category_id,
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
                                                'non_fungible_tokens' as category_id,
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
                                                'native_transfer' as category_id,
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
                                        'bridge' as category_id,
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
                                        category_id,
                                        'imx' as origin_key,
                                        SUM(txcount) as txcount 
                                from unioned 
                                group by 1,2
                        '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        

        ## This function is used for our Airtable setup - it returns the top unlabelled contracts (usage_category IS NULL) by gas fees
        def get_unlabelled_contracts(self, number_of_contracts, days):
                number_of_contracts = int(number_of_contracts)
                exec_string = f'''
                        WITH ranked_contracts AS (
                                SELECT
                                        cl.address, 
                                        cl.origin_key, 
                                        max(bl.deployment_date) AS deployment_date,
                                        max(bl.internal_description) AS internal_description,
                                        max(bl."name") AS name,
                                        bool_and(bl.is_proxy) AS is_proxy,
                                        max(bl.source_code_verified) AS source_code_verified,
                                        max(bl.owner_project) AS owner_project,
                                        SUM(cl.gas_fees_eth) AS gas_eth, 
                                        SUM(cl.txcount) AS txcount, 
                                        ROUND(AVG(cl.daa)) AS avg_daa,   
                                        AVG(cl.success_rate) AS avg_success,
                                        SUM(cl.gas_fees_eth)/SUM(cl.txcount) AS avg_contract_txcost_eth,
                                        ROW_NUMBER() OVER (PARTITION BY cl.origin_key ORDER BY SUM(gas_fees_eth) DESC) AS row_num_gas,
                                        ROW_NUMBER() OVER (PARTITION BY cl.origin_key ORDER BY SUM(daa) DESC) AS row_num_daa
                                FROM public.blockspace_fact_contract_level cl 
                                LEFT JOIN vw_oli_labels_materialized bl ON cl.address = bl.address AND cl.origin_key = bl.origin_key 
                                WHERE bl.usage_category IS NULL 
                                        AND cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        AND NOT EXISTS (
                                                SELECT 1
                                                FROM public.inscription_addresses ia
                                                WHERE ia.address = cl.address
                                        )
                                GROUP BY 1,2
                        ),
                        chain_txcost AS(
                                SELECT
                                        origin_key,
                                        AVG(value) AS avg_chain_txcost_median_eth
                                FROM public.fact_kpis
                                WHERE
                                        metric_key = 'txcosts_median_eth'
                                        AND "date" >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                GROUP BY origin_key
                        )
                        SELECT 
                                rc.address, 
                                rc.origin_key, 
                                rc.deployment_date,
                                rc.internal_description,
                                rc.name AS contract_name,
                                rc.is_proxy,
                                rc.source_code_verified,
                                rc.owner_project,
                                rc.gas_eth, 
                                rc.txcount, 
                                rc.avg_daa,
                                rc.avg_success,
                                rc.avg_contract_txcost_eth / mc.avg_chain_txcost_median_eth - 1 AS rel_cost
                        FROM ranked_contracts rc
                        LEFT JOIN chain_txcost mc ON rc.origin_key = mc.origin_key
                        WHERE row_num_gas <= {str(int(number_of_contracts/2))} OR row_num_daa <= {str(int(number_of_contracts/2))}
                        ORDER BY origin_key, row_num_gas, row_num_daa
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        # function that returns all contracts from inactive projects that need reassigning
        def get_inactive_contracts(self):
                exec_string = f'''
                        WITH inactive_names AS (
                                SELECT 
                                        "name",
                                        active,
                                        "source"
                                FROM public.oli_oss_directory
                                WHERE active = FALSE
                                ),

                                inactive_contracts AS(
                                        select
                                                value as owner_project,
                                        tm.address, 
                                        tm.origin_key
                                        FROM public.oli_tag_mapping tm
                                        JOIN inactive_names ia ON tm.value = ia."name"
                                        WHERE tm.tag_id = 'owner_project'
                                )

                                SELECT 
                                        ic.address, 
                                        ic.origin_key, 
                                        max(ic.owner_project) AS old_owner_project,
                                        max(bl.deployment_date) AS deployment_date,
                                        max(bl.internal_description) AS internal_description,
                                        max(bl.usage_category) AS usage_category, 
                                        max(bl."name") AS contract_name,
                                        bool_and(bl.is_proxy) AS is_proxy,
                                        max(bl.source_code_verified) AS source_code_verified
                                FROM inactive_contracts ic
                                LEFT JOIN vw_oli_labels_materialized bl ON ic.address = bl.address AND ic.origin_key = bl.origin_key
                                GROUP BY 1,2
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        ### Sys Chains functions
        # This function takes a dataframe with origin_key and an additional column as input and updates row-by-row the table sys_chains without overwriting other columns
        def update_sys_chains(self, df, column_type='str'):
                columns = df.columns.str.lower()
                if len(columns) != 2:
                        raise Exception("Only 2 columns are allowed in the dataframe")
                if 'origin_key' not in columns:
                        raise Exception("origin_key column is missing")
                
                value_column = columns[columns != 'origin_key'][0]

                ## for each row in the dataframe, create an update statement
                for index, row in df.iterrows():
                        if column_type == 'str':
                                exec_string = f"""
                                        UPDATE sys_chains
                                        SET {value_column} = '{row[value_column]}'
                                        WHERE origin_key = '{row['origin_key']}';
                                """
                        else:
                                raise NotImplementedError("Only string type is supported so far")
                        self.engine.execute(exec_string)
                print(f"{len(df)} projects updated in sys_chains")
                

        ### OLI functions
        def get_active_projects(self):
                # if multiple githubs, websites or socials, it will always take the first one in the list!
                exec_string = """
                        SELECT 
                                "name", 
                                display_name, 
                                description, 
                                replace((github->0->>'url'), 'https://github.com/', '') AS main_github,
                                replace(replace((social->'twitter'->0->>'url'), 'https://twitter.com/', ''),'https://x.com/', '') AS twitter,
                                (websites->0->>'url') AS website
                        FROM public.oli_oss_directory 
                        WHERE active = true
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_projects_for_airtable(self):
                exec_string = """
                        SELECT 
                                "name" AS "Name", 
                                display_name AS "Display Name", 
                                description AS "Description", 
                                replace((github->0->>'url'), 'https://github.com/', '') AS "Github" 
                        FROM public.oli_oss_directory
                        WHERE active = True;
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        def deactivate_projects(self, names:list):
                exec_string = f"""
                        UPDATE oli_oss_directory
                        SET active = false
                        WHERE name IN ({', '.join([f"'{name}'" for name in names])})
                """
                self.engine.execute(exec_string)
                print(f"{len(names)} projects deactivated in oli_oss_directory: {names}")

        def get_tags_inactive_projects(self):
                exec_string = """
                        with active_projects as (
                                select * 
                                from oli_oss_directory ood 
                                where active = true 
                        )

                        select otm.*
                        from oli_tag_mapping otm 
                        left join active_projects ip on ip.name = otm.value
                        where tag_id = 'owner_project'
                        and ip.name is null
                        order by value asc
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        ## This function is used to generate the API endpoints for the OLI labels
        def get_oli_labels(self, chain_id='origin_key'):
                if chain_id == 'origin_key':
                        chain_str = 'origin_key'
                elif chain_id == 'caip2':
                        chain_str = 'caip2 as chain_id'
                else:
                        raise ValueError("chain_id must be either 'origin_key' or 'caip2'")
                
                exec_string = f"""
                        SELECT 
                                address,
                                {chain_str},
                                name,
                                owner_project,
                                usage_category,
                                is_factory_contract,
                                deployment_tx,
                                deployer_address,
                                deployment_date
                        FROM public.vw_oli_labels_materialized
                        LEFT JOIN sys_chains USING (origin_key)
                        WHERE owner_project IS NOT NULL
                        """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        ## TODO: filter by contracts only?
        def get_labels_lite_db(self, limit=50000, order_by='txcount', origin_keys=None):
                exec_string = f"""
                        with prev_period as (
                                SELECT 
                                        cl.address, 
                                        cl.origin_key, 
                                        sum(txcount) as txcount, 
                                        sum(gas_fees_usd) as gas_fees_usd, 	
                                        sum(daa) as daa	
                                FROM public.blockspace_fact_contract_level cl
                                where "date"  >= current_date - interval '14 days'
                                        and "date" < current_date - interval '7 days'
                                group by 1,2
                        )

                        , current_period as (
                                SELECT 
                                        cl.address, 
                                        cl.origin_key, 
                                        sum(txcount) as txcount, 
                                        sum(gas_fees_usd) as gas_fees_usd, 	
                                        sum(daa) as daa	
                                FROM public.blockspace_fact_contract_level cl
                                where "date"  >= current_date - interval '7 days'
                                        and "date" < current_date
                                group by 1,2
                        )

                        SELECT 
                                cl.address, 
                                cl.origin_key, 
                                syc.caip2 as chain_id,
                                lab."name",
                                lab.owner_project,
                                oss.display_name as owner_project_clear,
                                lab.usage_category,
                                lab.deployment_tx,
                                lab.deployer_address,
                                lab.deployment_date,
                                cl.txcount as txcount,
                                (cl.txcount - prev.txcount) / prev.txcount as txcount_change,
                                cl.gas_fees_usd as gas_fees_usd, 	
                                (cl.gas_fees_usd - prev.gas_fees_usd) / prev.gas_fees_usd as gas_fees_usd_change,
                                cl.daa as daa,
                                (cl.daa - prev.daa) / prev.daa as daa_change
                        FROM current_period cl
                        left join prev_period prev using (address, origin_key)
                        left join vw_oli_labels_materialized lab using (address, origin_key)
                        left join oli_oss_directory oss on oss.name = lab.owner_project
                        left join sys_chains syc on cl.origin_key = syc.origin_key
                        where cl.origin_key IN ('{"','".join(origin_keys)}')
                                and (lab.owner_project is null OR oss.active = true)
                        order by {order_by} desc
                        limit {limit}
                """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_labels_export_df(self, limit=50000, origin_keys=None, incl_aggregation=True):
                if incl_aggregation:
                        aggregation = """
                                ,sum(txcount) as txcount_180d 
                                ,sum(gas_fees_usd) as gas_fees_usd_180d
                        """
                else:
                        aggregation = ""

                exec_string = f"""
                        SELECT 
                                cl.address, 
                                cl.origin_key, 
                                syc.caip2 as chain_id,
                                lab."name",
                                lab.owner_project,
                                lab.usage_category,
                                lab.deployment_tx,
                                lab.deployer_address,
                                lab.deployment_date
                                {aggregation}
                        FROM public.blockspace_fact_contract_level cl
                        left join vw_oli_labels_materialized lab using (address, origin_key)
                        left join sys_chains syc on cl.origin_key = syc.origin_key
                        where cl."date"  >= current_date - interval '180 days'
                                and cl."date" < current_date
                                and (lab."name" is not null OR lab.owner_project is not null OR lab.deployment_tx is not null OR lab.deployer_address is not null OR lab.deployment_date is not null)
                                and cl.origin_key IN ('{"','".join(origin_keys)}')
                        group by 1,2,3,4,5,6,7,8,9
                        order by sum(txcount) desc
                        limit {limit}
                """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df

        def get_labels_page_sparkline(self, limit=100, origin_keys=None):
                exec_string = f"""
                        with top as (
                                SELECT 
                                address, 
                                origin_key, 
                                sum(txcount) as txcount_limit
                        FROM public.blockspace_fact_contract_level
                        where "date"  >= date_trunc('day',now()) - interval '7 days'
                                and "date" < date_trunc('day', now())
                                and origin_key IN ('{"','".join(origin_keys)}')
                        group by 1,2
                        order by 3 desc
                        limit {limit}
                        )

                        SELECT 
                                address, 
                                origin_key,
                                "date",
                                sum(txcount) as txcount, 
                                sum(gas_fees_usd) as gas_fees_usd, 	
                                sum(daa) as daa
                        FROM blockspace_fact_contract_level
                        inner join top using (address, origin_key)
                        where "date"  >= date_trunc('day',now()) - interval '30 days'
                                and "date" < date_trunc('day', now())
                        group by 1,2,3
                """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_glo_holders(self):
                exec_string = f"""
                        with max_date as (
                                select max("date") as "date" from glo_holders
                        )

                        SELECT address, balance
                        FROM public.glo_holders
                        inner join max_date using ("date")
                        order by 2 desc
                        """
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_glo_mcap(self):
                exec_string = f"""
                        select "date", metric_key, value  from fact_kpis
                        where origin_key = 'glo-dollar'
                        and metric_key in ('market_cap_usd', 'market_cap_eth')
                        and value > 0
                """

                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_special_use_rpc(self, origin_key:str):
                try:
                        query = f"SELECT url FROM sys_rpc_config WHERE origin_key = '{origin_key}' and special_use = true LIMIT 1"
                        with self.engine.connect() as connection:
                                result = connection.execute(query)
                                rpc = result.scalar()
                                return rpc
                except Exception as e:
                        print(f"Error retrieving a synced rpc for {origin_key}.")
                        print(e)
                        return None
                
        def get_block_by_date(self, table_name:str, date:datetime):
                # Check if the table has a 'block_timestamp' column
                insp = sqlalchemy.inspect(self.engine)
                columns = [col['name'] for col in insp.get_columns(table_name)]
                
                if 'block_timestamp' not in columns:
                        raise ValueError(f"Table {table_name} does not have a 'block_timestamp' column.")
                
                # Format the date to ensure it's in the correct format for SQL
                date_str = date.strftime('%Y-%m-%d')
                
                # Execute the query to get the block number for the given date
                exec_string = f"""
                SELECT MIN(block_number) as block_number 
                FROM {table_name} 
                WHERE date_trunc('day', block_timestamp) = '{date_str}';
                """
                
                with self.engine.connect() as connection:
                        result = connection.execute(exec_string)
                        for row in result:
                                block_number = row['block_number']
                                
                if block_number is None:
                        raise ValueError(f"No block found for the date {date_str} in table {table_name}.")
                
                return block_number