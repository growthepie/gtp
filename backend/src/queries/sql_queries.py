from jinja2 import Environment, FileSystemLoader, StrictUndefined

env = Environment(loader=FileSystemLoader('src/queries/postgres'), undefined=StrictUndefined)

sql_q= {

        ### Celestia
        'celestia_da_data_posted_bytes': """
        select 
                date_trunc('day', block_timestamp) as day, 
                sum(blob_sizes) as value
        from (
                SELECT 
                        block_timestamp, 
                        jsonb_array_elements(blob_sizes::jsonb)::numeric as blob_sizes
                FROM celestia_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ) a
        group by 1
        """

        ,'celestia_da_fees_eth': """
        with tia_price as (
                SELECT "timestamp", value as price_eth
                FROM public.fact_kpis_granular
                where origin_key = 'celestia' and metric_key = 'price_eth' and granularity = 'hourly'
                        and timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        )

        select 
                date_trunc('day', block_timestamp) as day, 
                sum(fee * price_eth)/1e6 as value
        FROM celestia_tx tx
        left join tia_price p on date_trunc('hour', tx.block_timestamp) = p.timestamp
        where blob_sizes is not null 
                and block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        group by 1
        """

        ,'celestia_da_unique_blob_producers': """
        select 
                date_trunc('day', block_timestamp) as day, 
                COUNT(DISTINCT namespaces) AS value
        from (
                SELECT 
                        block_timestamp, 
                        jsonb_array_elements(namespaces::jsonb)::text as namespaces
                FROM celestia_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ) a
        group by 1
        """

        ,'celestia_da_blob_count': """
        SELECT 
                date_trunc('day', block_timestamp) as day, 
                COUNT(*) as value
        FROM public.celestia_tx
        WHERE 
                block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                and blob_sizes IS NOT NULL
        group by 1
        """
        
        # Starknet
        
        ,'starknet_user_base_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM starknet_tx tx
        WHERE
                block_timestamp < date_trunc('{{aggregation}}', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'starknet_txcosts_median_eth': """
        WITH 
        starknet_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.starknet_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.median_tx_fee as value
        FROM starknet_median z
        """

        ,'starknet_fees_paid_eth': """
        WITH starknet_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.starknet_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee AS value
        FROM starknet_tx_filtered z
        """

        ,'starknet_rent_paid_eth': """
        SELECT 
                date as day,
                SUM(value) as value
        FROM public.fact_kpis
        WHERE origin_key = 'starknet' and metric_key in ('l1_data_availability_eth', 'l1_settlement_custom_eth', 'ethereum_blobs_eth')
                AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                AND "date" > '2024-02-25'
        GROUP BY 1
        """
}

class SQLQuery():
    def __init__(self, jinja_path: str, metric_key: str, origin_key: str, query_parameters: dict = None, currency_dependent:bool = True):
        self.template = env.get_template(jinja_path)
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters if query_parameters is not None else {}
        self.query_parameters['origin_key'] = origin_key
        self.currency_dependent = currency_dependent ## if false, the query can in parellel to the currency queries 

## Queries have default values defined in the jinja templates. These can either be overwritten here or later in adater_sql.py
sql_queries = [
        # Multi-chain
        SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", jinja_path='chain_metrics/select_user_base_weekly.sql.j2', currency_dependent = False)

        ## Ethereum
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "ethereum", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "ethereum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)

        # Arbitrum Orbit
        # Different filter col on txcounts (gas_used instead of gas_price)
        ## Arbitrum
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "arbitrum", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)

        # SUPERCHAIN
        # TODO just create queries based on for loop?
        ## OP Mainnet
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "optimism", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "optimism", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "optimism", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "optimism", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "optimism", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "optimism", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "optimism", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "optimism", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "optimism", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "optimism", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Base
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "base", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "base", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "base", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "base", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "base", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "base", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "base", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "base", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "base", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "base", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Zora
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "zora", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "zora", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "zora", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "zora", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "zora", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "zora", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "zora", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "zora", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "zora", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "zora", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Mode
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "mode", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "mode", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "mode", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "mode", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mode", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mode", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mode", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "mode", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mode", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mode", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Redstone
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "redstone", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "redstone", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "redstone", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "redstone", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "redstone", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "redstone", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "redstone", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "redstone", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "redstone", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "redstones", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        ## Derive
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "derive", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "derive", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "derive", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "derive", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "derive", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "derive", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "derive", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "derive", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "derive", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "derive", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "celestia_blob_size_bytes", origin_key = "derive", jinja_path='chain_metrics/select_celestia_blob_size_bytes.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "celestia_blobs_eth", origin_key = "derive", jinja_path='chain_metrics/select_celestia_blobs.sql.j2')

        ## Orderly
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "orderly", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "orderly", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "orderly", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "orderly", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "orderly", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "orderly", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "orderly", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "orderly", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "orderly", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "orderly", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "celestia_blob_size_bytes", origin_key = "orderly", jinja_path='chain_metrics/select_celestia_blob_size_bytes.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "celestia_blobs_eth", origin_key = "orderly", jinja_path='chain_metrics/select_celestia_blobs.sql.j2')

        ## Worldchain
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "worldchain", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "worldchain", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "worldchain", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "worldchain", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "worldchain", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "worldchain", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "worldchain", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "worldchain", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "worldchain", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "worldchain", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        # Elastic Chain
        ## ZKsync Era
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "zksync_era", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "zksync_era", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "zksync_era", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "zksync_era", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "zksync_era", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "zksync_era", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "zksync_era", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "zksync_era", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "zksync_era", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "zksync_era", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        # Polygon zkStack
        ## Polygon zkEVM
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        # Others EVM
        ## Linea
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "linea", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "linea", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "linea", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "linea", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "linea", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "linea", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "linea", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "linea", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "linea", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "linea", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Scroll
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "scroll", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "scroll", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "scroll", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "scroll", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "scroll", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "scroll", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "scroll", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "scroll", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "scroll", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "scroll", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        
        ## Blast
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "blast", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "blast", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "blast", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "blast", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "blast", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "blast", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "blast", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "blast", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "blast", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "blast", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        ## Taiko
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "taiko", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "taiko", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "taiko", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "taiko", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "taiko", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "taiko", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "taiko", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "taiko", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "taiko", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "taiko", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        ## Manta
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "manta", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "manta", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "manta", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "manta", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "manta", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "manta", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "manta", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "manta", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "manta", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "manta", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "celestia_blob_size_bytes", origin_key = "manta", jinja_path='chain_metrics/select_celestia_blob_size_bytes.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "celestia_blobs_eth", origin_key = "manta", jinja_path='chain_metrics/select_celestia_blobs.sql.j2')

        # Others EVM Custom Gas Token
        ## Mantle (also custom gas query)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "mantle", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "mantle", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mantle", jinja_path='chain_metrics/custom/mantle_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "mantle", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        ## Metis
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "metis", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "metis", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "metis", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "metis", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "metis", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "metis", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        ## Gravity (Orbit tx filter, also custom gas query)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "gravity", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "gravity", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "gravity", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "gravity", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gravity", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gravity", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        # Others Non-EVM
        ## IMX
        ,SQLQuery(metric_key = "txcount", origin_key = "imx", jinja_path='chain_metrics/custom/imx_select_txcount.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "imx", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "imx", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "imx", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "imx", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "imx", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)

        ## Loopring
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "loopring", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "loopring", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "loopring", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)

        ## Rhino
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "rhino", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "rhino", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "rhino", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "rhino", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "rhino", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "rhino", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca", origin_key = "rhino", jinja_path='chain_metrics/select_cca.sql.j2', currency_dependent = False)

        ## Starknet
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "starknet", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "starknet", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
]