sql_q= {
        ## profit usd
        'profit_usd': """
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

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'linea' as chain
                FROM linea_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                -- UNION ALL
                
                -- SELECT 
                -- DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                -- from_address as address,
                -- 'mantle' as chain
                -- FROM mantle_tx
                -- WHERE
                -- block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                -- AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                -- UNION ALL
                
                -- SELECT 
                -- DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                -- from_address as address,
                -- 'scroll' as chain
                -- FROM scroll_tx
                -- WHERE
                -- block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                -- AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

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


        ### IMX
        ## count of all actions that have a transaction_id (not orders!)
        ,'imx_txcount': """ 
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


        ### Arbitrum
        ,'arbitrum_txcount_raw': """
        SELECT date_trunc('day', at2.block_timestamp) AS day,
                count(*) AS value,
                'arbitrum' AS origin_key
        FROM arbitrum_tx at2
        WHERE at2.gas_used > 0
                AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', at2.block_timestamp))

        """

        ### OP Mainnet
        ,'optimism_txcount_raw': """
        SELECT date_trunc('day', ot.block_timestamp) AS day,
                count(*) AS value,
                'optimism' AS origin_key
        FROM optimism_tx ot
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', ot.block_timestamp))
        """

        ### Base
        ,'base_txcount_raw': """
        SELECT date_trunc('day', bt.block_timestamp) AS day,
                count(*) AS value,
                'base' AS origin_key
        FROM base_tx bt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', bt.block_timestamp))
        """

        ### Polygon zkEVM
        ,'polygon_zkevm_txcount_raw': """
        SELECT date_trunc('day', pzt.block_timestamp) AS day,
                count(*) AS value,
                'polygon_zkevm' AS origin_key
        FROM polygon_zkevm_tx pzt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', pzt.block_timestamp))
        """

        ### zkSync Era
        ,'zksync_era_txcount_raw': """
        SELECT date_trunc('day', zet.block_timestamp) AS day,
                count(*) AS value,
                'zksync_era' AS origin_key
        FROM zksync_era_tx zet
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', zet.block_timestamp))
        """

        ### Zora
        ,'zora_txcount_raw': """
        SELECT date_trunc('day', zt.block_timestamp) AS day,
                count(*) AS value,
                'zora' AS origin_key
        FROM zora_tx zt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', zt.block_timestamp))
        """

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

        ,'zora_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.zora_tx
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

        ### PGN
        ,'pgn_fees_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value,
                'gitcoin_pgn' AS origin_key
        FROM gitcoin_pgn_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', gpt.block_timestamp))
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

    ,'pgn_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.gitcoin_pgn_tx
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

        ### Linea
        ,'linea_txcount_raw': """
        SELECT date_trunc('day', linea_tx.block_timestamp) AS day,
                count(*) AS value,
                'linea' AS origin_key
        FROM linea_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', linea_tx.block_timestamp))
        """

    ,'linea_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.linea_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'linea_fees_paid_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        linea_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.linea_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee * e.price_usd AS value
        --,z.total_tx_fee AS fees_paid_eth
        FROM linea_tx_filtered z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
    """

    ,'linea_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.linea_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'linea_txcosts_median_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        linea_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.linea_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.median_tx_fee * e.price_usd as value
                --,z.median_tx_fee as txcosts_median_eth
        FROM linea_median z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
   """

        ### Mantle
        ,'mantle_txcount_raw': """
        SELECT date_trunc('day', mt.block_timestamp) AS day,
                count(*) AS value,
                'mantle' AS origin_key
        FROM mantle_tx mt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', mt.block_timestamp))
        """

        ,'mantle_fees_paid_usd': """
        WITH mnt_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'MNT' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        mantle_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.mantle_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                mantle.day,
                mantle.total_tx_fee * e.price_usd AS value
        --,mantle.total_tx_fee AS fees_paid_mnt
        FROM mantle_tx_filtered mantle
        LEFT JOIN mnt_price e ON mantle.day = e."date"
        ORDER BY mantle.day DESC
    """
        ,'mantle_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.mantle_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """
        ,'mantle_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.mantle_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """
        ,'mantle_txcosts_median_usd': """
        WITH mnt_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'MNT' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        mantle_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.mantle_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                mantle.day,
                mantle.median_tx_fee * e.price_usd as value
                --,mantle.median_tx_fee as txcosts_median_mnt
        FROM mantle_median mantle
        LEFT JOIN mnt_price e ON mantle.day = e."date"
        ORDER BY mantle.day DESC
    """

    # Scroll
        ,'scroll_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value,
                'scroll' AS origin_key
        FROM scroll_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', st.block_timestamp))
        """

        ,'scroll_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.scroll_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
        """

        ,'scroll_fees_paid_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        scroll_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.scroll_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee * e.price_usd AS value
        FROM scroll_tx_filtered z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
    """

    ,'scroll_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.scroll_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
    """

    ,'scroll_txcosts_median_usd': """
        WITH eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        scroll_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.scroll_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.median_tx_fee * e.price_usd as value
        FROM scroll_median z
        LEFT JOIN eth_price e ON z.day = e."date"
        ORDER BY z.day DESC
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
    ## Multichain
    SQLQuery(metric_key = "profit_usd", origin_key = "multi", sql=sql_q["profit_usd"], query_parameters={"Days": 7})
    # ,SQLQuery(metric_key = "user_base_daily", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7, "aggregation": "day"})
    ,SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4, "aggregation": "week"})
    # ,SQLQuery(metric_key = "user_base_monthly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4*12, "aggregation": "month"})


    ## IMX
    ,SQLQuery(metric_key = "txcount", origin_key = "imx", sql=sql_q["imx_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "imx", sql=sql_q["imx_daa"], query_parameters={"Days": 7})
    #,SQLQuery(metric_key = "new_addresses", origin_key = "imx", sql=sql_q["ethereum_new_addresses"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "imx", sql=sql_q["imx_fees_paid_usd"], query_parameters={"Days": 7})

    ## Arbitrum
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum", sql=sql_q["arbitrum_txcount_raw"], query_parameters={"Days": 30})

    ## OP Mainnet
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "optimism", sql=sql_q["optimism_txcount_raw"], query_parameters={"Days": 30})

    ## Base
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "base", sql=sql_q["base_txcount_raw"], query_parameters={"Days": 30})

    ## Polygon zkEVM
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_txcount_raw"], query_parameters={"Days": 30})

    ## zkSync Era
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "zksync_era", sql=sql_q["zksync_era_txcount_raw"], query_parameters={"Days": 30})

    ## Zora
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "zora", sql=sql_q["zora_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "zora", sql=sql_q["zora_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "zora", sql=sql_q["zora_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "zora", sql=sql_q["zora_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "zora", sql=sql_q["zora_txcosts_median_usd"], query_parameters={"Days": 7})

    ## PGN
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcosts_median_usd"], query_parameters={"Days": 7})
    
    ## Linea
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "linea", sql=sql_q["linea_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "linea", sql=sql_q["linea_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "linea", sql=sql_q["linea_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "linea", sql=sql_q["linea_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "linea", sql=sql_q["linea_txcosts_median_usd"], query_parameters={"Days": 7})

    ## Mantle
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "mantle", sql=sql_q["mantle_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "mantle", sql=sql_q["mantle_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "mantle", sql=sql_q["mantle_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "mantle", sql=sql_q["mantle_txcosts_median_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "mantle", sql=sql_q["mantle_fees_paid_usd"], query_parameters={"Days": 7})

    ## Scroll
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "scroll", sql=sql_q["scroll_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "scroll", sql=sql_q["scroll_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "scroll", sql=sql_q["scroll_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "fees_paid_usd", origin_key = "scroll", sql=sql_q["scroll_fees_paid_usd"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_usd", origin_key = "scroll", sql=sql_q["scroll_txcosts_median_usd"], query_parameters={"Days": 7})
   
]