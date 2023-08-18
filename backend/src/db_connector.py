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
        def __init__(self):
            self.url = f"postgresql+psycopg2://{db_user}:{db_passwd}@{db_host}/{db_name}"
            self.engine = sqlalchemy.create_engine(
                self.url,
                connect_args={
                        "keepalives": 1,
                        "keepalives_idle": 30,
                        "keepalives_interval": 10,
                        "keepalives_count": 5,
                } 
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
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        select
                                to_address as address,
                                date_trunc('day', block_timestamp) as date,
                                sum(tx_fee) as gas_fees_eth,
                                sum(tx_fee * p.value) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa,
                                '{chain}' as origin_key
                        from {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and empty_input = false -- we don't have to store addresses that received native transfers
                                and to_address <> '' and to_address is not null -- filter out contract creations arbitrum, optimism
                                and to_address <> '\\x0000000000000000000000000000000000008006' -- filter out contract creations zksync
                        group by 1,2
                        having count(*) > 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_native_transfers(self, chain, days):
                ## native transfers: all transactions that have no input data
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'native_transfer' as sub_category_key,
                                '{chain}' as origin_key,
                                sum(tx_fee) as gas_fees_eth,
                                sum(tx_fee * p.value) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                and empty_input = true
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_contract_deplyments(self, chain, days):
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

                        SELECT 
                                date_trunc('day', block_timestamp) as date,
                                'smart_contract_deployment' as sub_category_key,
                                '{chain}' as origin_key,
                                sum(tx_fee) as gas_fees_eth,
                                sum(tx_fee * p.value) as gas_fees_usd,
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        FROM {chain}_tx tx 
                        LEFT JOIN eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                {filter_string}
                        group by 1
                '''
                df = pd.read_sql(exec_string, self.engine.connect())
                return df
        
        def get_blockspace_total(self, chain, days):
                exec_string = f'''
                        with eth_price as (
                                SELECT "date", value
                                FROM fact_kpis
                                WHERE metric_key = 'price_usd' and origin_key = 'ethereum'
                        )

                        select 
                                date_trunc('day', block_timestamp) as date,
                                'total_usage' as sub_category_key,
                                '{chain}' as origin_key,
                                sum(tx_fee) as gas_fees_eth,
                                sum(tx_fee * p.value) as gas_fees_usd, 
                                count(*) as txcount,
                                count(distinct from_address) as daa
                        from {chain}_tx tx
                        left join eth_price p on date_trunc('day', tx.block_timestamp) = p."date"
                        where block_timestamp < DATE_TRUNC('day', NOW())
                                and block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
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
        
        """
        This function is used to get the top contracts by category for the blockspace dashboard
        category_type: main_category or sub_category
        category: the category key (e.g. dex, unlabeled, etc.)
        chain: arbitrum, optimism, polygon_zkevm, etc. OR all
        top_by: gas or txcount
        days: 7, 30, 90, 180, 365
        
        """
        def get_top_contracts_by_category(self, category_type, category, chain, top_by, days):
                if chain == 'all':
                        chain_string = ''
                else:
                        chain_string = f"and cl.origin_key = '{chain}'"

                if category_type == 'main_category':
                        category_string = 'bcm.main_category_key'
                elif category_type == 'sub_category':
                        category_string = 'bl.sub_category_key'
                else:
                        print('invalid category type')
                        raise ValueError
                
                if top_by == 'gas':
                        top_by_string = 'gas_fees_eth'
                elif top_by == 'txcount':
                        top_by_string = 'txcount'

                if category == 'unlabeled':
                        exec_string = f'''
                                SELECT 
                                        cl.address,
                                        cl.origin_key,
                                        null as contract_name,
                                        null as project_name,
                                        null as sub_category_key,
                                        null as sub_category_name,
                                        'unlabeled' as main_category_key,
                                        'Unlabeled' as main_category_name,
                                        sum(gas_fees_eth) as gas_fees_eth,
                                        sum(gas_fees_usd) as gas_fees_usd,
                                        sum(txcount) as txcount,
                                        round(avg(daa)) as daa
                                FROM public.blockspace_fact_contract_level cl
                                left join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                where 
                                        bl.address is null
                                        and date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        {chain_string}
                                group by 1,2,3,4,5,6,7,8
                                order by {top_by_string} desc
                                limit 100
                        '''

                else:
                        exec_string = f'''
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
                                inner join blockspace_labels bl on cl.address = bl.address and cl.origin_key = bl.origin_key 
                                inner join blockspace_category_mapping bcm on lower(bl.sub_category_key) = lower(bcm.sub_category_key) 
                                where 
                                        date < DATE_TRUNC('day', NOW())
                                        and date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                                        {chain_string}
                                        and lower({category_string}) = lower('{category}')
                                group by 1,2,3,4,5,6,7,8
                                order by {top_by_string} desc
                                limit 100
                        '''


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
