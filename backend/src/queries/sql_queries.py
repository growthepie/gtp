sql_q= {
    ## count of all actions that have a transaction_id (not orders!)
    'imx_txcount': """ 
        with 
        cte_imx_deposits as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'deposits' as tx_type
                from imx_deposits
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
                order by 1
        ),	
        cte_imx_mints as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'mints' as tx_type
                from imx_mints
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
                order by 1
        ),    
        cte_imx_trades as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'trades' as tx_type
                from imx_trades
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
                order by 1
        ),    
        cte_imx_transfers as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'transfers' as tx_type
                from imx_transfers
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
                order by 1
        ),
        cte_imx_withdrawals as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'withdrawals' as tx_type
                from imx_withdrawals  
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
                order by 1
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
                select * from cte_imx_transfers 
        )
        select 
                day, SUM(value) as val 
        from unioned 
        group by 1
        order by 1 desc
    """
    
    ## count of all addresses that activels interacted on imx (not mints, because they are not triggered by users themselves). Only fullfilled orders are counted as well.
    ,'imx_daa': """
         with 
        cte_imx_deposits as (
                select 
                        date_trunc('day', "timestamp") as day 
                        , "user" as address
                        , 'deposits' as tx_type
                from imx_deposits
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
        ),
        cte_imx_withdrawals as (
                select 
                        date_trunc('day', "timestamp") as day 
                        , "sender" as address
                        , 'withdrawals' as tx_type
                from imx_withdrawals  
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
        ),
        cte_imx_orders as (
                select 
                        date_trunc('day', "updated_timestamp") as day 
                        , "user" as address
                        , 'orders' as tx_type
                from imx_orders   
                WHERE updated_timestamp < date_trunc('day', now())
                        AND updated_timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
        ),
        cte_imx_transfers as (
                select 
                        date_trunc('day', "timestamp") as day
                        , "user" as address
                        , 'transfers' as tx_type
                from imx_transfers
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
        ),    
        unioned as (
                select * from cte_imx_deposits
                union all
                select * from cte_imx_withdrawals
                union all
                select * from cte_imx_orders
                union all
                select * from cte_imx_transfers
        )
        
        select
                day,
                Count(distinct address) as val
        from unioned
        group by 1
        order by 1 desc
    """

    ,'imx_fees_paid_usd': """
        SELECT 
                date_trunc('day', "updated_timestamp") as day,
                SUM((amount::decimal / power(10,it.decimals)) * pd.price_usd) as val
        FROM public.imx_fees f
        left join prices_daily pd 
                on date_trunc('day', "updated_timestamp") = pd."date" 
                and f.token_data_contract_address = pd.token_address 
        left join imx_tokens it on f.token_data_contract_address = it.token_address 
        where "type" = 'protocol'
                and updated_timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                and updated_timestamp < date_trunc('day', now())
        group by 1
        order by 1 desc
    """

    ## multichain users
    ,'user_base_xxx': """
        with raw_data as (   
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'arbitrum' as chain
                FROM arbitrum_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL  '{{Days}} days')
                
                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'optimism' as chain
                FROM optimism_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')
                
                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'polygon_zkevm' as chain
                FROM polygon_zkevm_tx 
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'zksync_era' as chain
                FROM zksync_era_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'base' as chain
                FROM base_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'zora' as chain
                FROM zora_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'gitcoin_pgn' as chain
                FROM gitcoin_pgn_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

        -- IMX   
        UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', "timestamp") AS day,
                "user" as address,
                'imx' as chain
                FROM imx_deposits id 
                WHERE
                "timestamp" < DATE_TRUNC('{{aggregation}}', NOW())
                AND "timestamp" >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')
                
        UNION ALL
                
                select 
                        date_trunc('{{aggregation}}', "timestamp") as day 
                        , "sender" as address
                        , 'imx' as chain
                from imx_withdrawals  
                WHERE timestamp < date_trunc('{{aggregation}}', now())
                        AND timestamp >= date_trunc('{{aggregation}}',now() - INTERVAL '{{Days}} days')

        union all 
        
        select 
                        date_trunc('{{aggregation}}', "updated_timestamp") as day 
                        , "user" as address
                        , 'imx' as chain
                from imx_orders   
                WHERE updated_timestamp < date_trunc('{{aggregation}}', now())
                        AND updated_timestamp >= date_trunc('{{aggregation}}',now() - INTERVAL '{{Days}} days')
                        
        union all 
        
        select 
                date_trunc('{{aggregation}}', "timestamp") as day
                , "user" as address
                , 'imx' as chain
                from imx_transfers
                WHERE timestamp < date_trunc('{{aggregation}}', now())
                        AND timestamp >= date_trunc('{{aggregation}}',now() - INTERVAL '{{Days}} days')
                
        )
        ,chain_info as (
                SELECT 
                day,
                address,
                CASE WHEN count(distinct chain) > 1 THEN 'multiple' ELSE MAX(chain) END as origin_key
                FROM raw_data
                GROUP BY 1,2
        )

        SELECT
                day,
                origin_key,
                COUNT(DISTINCT address) AS val
        FROM
                chain_info
        GROUP BY 1,2 
        ORDER BY 1 DESC
        """

    ## profit usd
    , 'profit_usd': """
        with tmp as (
        SELECT 
                date,
                origin_key,
                SUM(CASE WHEN metric_key = 'rent_paid_usd' THEN value END) AS rent_paid_usd,
                SUM(CASE WHEN metric_key = 'fees_paid_usd' THEN value END) AS fees_paid_usd
        FROM fact_kpis
        WHERE metric_key = 'rent_paid_usd' or metric_key = 'fees_paid_usd'
                AND date >= date_trunc('day',now()) - interval '{{Days}} days'
                AND date < date_trunc('day', now())
        GROUP BY 1,2
        )

        SELECT
                date as day, 
                origin_key,
                fees_paid_usd - rent_paid_usd as value 
        FROM tmp
        WHERE rent_paid_usd > 0 and fees_paid_usd > 0
        ORDER BY 1 desc

        """

## Zora & PGN 

    ,'zora_fees_paid_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        zora_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.zora_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee * e.price_usd AS value
        --,z.total_tx_fee AS fees_paid_eth
        FROM zora_tx_filtered z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
    """

    ,'pgn_fees_paid_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        pgn_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.gitcoin_pgn_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                pgn.day,
                pgn.total_tx_fee * e.price_usd AS value
        --,pgn.total_tx_fee AS fees_paid_eth
        FROM pgn_tx_filtered pgn
        LEFT JOIN eth_price e ON pgn.day = e."date"
        ORDER BY pgn.day DESC
    """

    ,'zora_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.zora_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'pgn_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.gitcoin_pgn_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'zora_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.zora_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'pgn_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.gitcoin_pgn_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'zora_txcosts_median_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        zora_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.zora_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.median_tx_fee * e.price_usd as value
                --,z.median_tx_fee as txcosts_median_eth
        FROM zora_median z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
    """

    ,'pgn_txcosts_median_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        pgn_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.gitcoin_pgn_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                pgn.day,
                pgn.median_tx_fee * e.price_usd as value
                --,pgn.median_tx_fee as txcosts_median_eth
        FROM pgn_median pgn
        LEFT JOIN eth_price e ON pgn.day = e."date"
        ORDER BY pgn.day DESC
    """
}


class SQLObject():
    ## replace_query_parameters
    def replace_query_parameters(self, sql: str, query_parameters: dict) -> str:
        for key, value in query_parameters.items():
            sql = sql.replace("{{" + key + "}}", str(value))
        return sql  

    def update_query_parameters(self, query_parameters: dict):
        for key in query_parameters.keys():
            self.query_parameters[key] = query_parameters[key]
        # self.query_parameters = query_parameters
        self.sql = self.replace_query_parameters(self.sql_raw, self.query_parameters)

class SQLQuery(SQLObject):
    def __init__(self, sql: str, metric_key: str, origin_key: str, query_parameters: dict = None):
        self.sql_raw = sql 
        self.sql = self.replace_query_parameters(self.sql_raw, query_parameters)
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.last_token = None
        self.last_execution_loaded = None

sql_queries = [
    ## IMX
    SQLQuery(metric_key = "txcount", origin_key = "imx", sql=sql_q["imx_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "imx", sql=sql_q["imx_daa"], query_parameters={"Days": 7})
    #,SQLQuery(metric_key = "new_addresses", origin_key = "imx", sql=sql_q["ethereum_new_addresses"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "imx", sql=sql_q["imx_fees_paid_usd"], query_parameters={"Days": 7})

    ## Zora & PGN
    ,SQLQuery(metric_key = "txcount", origin_key = "zora", sql=sql_q["zora_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcount", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "zora", sql=sql_q["zora_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "zora", sql=sql_q["zora_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "zora", sql=sql_q["zora_txcosts_median_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcosts_median_usd"], query_parameters={"Days": 7})
    

    ## Multichain
    ,SQLQuery(metric_key = "profit_usd", origin_key = "multi", sql=sql_q["profit_usd"], query_parameters={"Days": 7})
    # ,SQLQuery(metric_key = "user_base_daily", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7, "aggregation": "day"})
    ,SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4, "aggregation": "week"})
    # ,SQLQuery(metric_key = "user_base_monthly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4*12, "aggregation": "month"})

   
]