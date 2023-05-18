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
    ## Layer 1s
    SQLQuery(metric_key = "txcount", origin_key = "imx", sql=sql_q["imx_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "imx", sql=sql_q["imx_daa"], query_parameters={"Days": 7})
    #,SQLQuery(metric_key = "new_addresses", origin_key = "imx", sql=sql_q["ethereum_new_addresses"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "imx", sql=sql_q["imx_fees_paid_usd"], query_parameters={"Days": 7})

    # ,SQLQuery(metric_key = "user_base_daily", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7, "aggregation": "day"})
    ,SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4, "aggregation": "week"})
    # ,SQLQuery(metric_key = "user_base_monthly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4*12, "aggregation": "month"})

]