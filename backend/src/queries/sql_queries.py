sql_q= {
        ## multichain users
        'user_base_xxx': """
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

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'mantle' as chain
                FROM mantle_tx
                WHERE
                block_timestamp < DATE_TRUNC('{{aggregation}}', NOW())
                AND block_timestamp >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL '{{Days}} days')

                UNION ALL
                
                SELECT 
                DATE_TRUNC('{{aggregation}}', block_timestamp) AS day,
                from_address as address,
                'scroll' as chain
                FROM scroll_tx
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

    ## count of all addresses that actively interacted on imx (not mints, because they are not triggered by users themselves). Only fullfilled orders are counted as well.
    ,'imx_maa': """
         with 
        cte_imx_deposits as (
                select 
                        date_trunc('month', "timestamp") as day 
                        , "user" as address
                        , 'deposits' as tx_type
                from imx_deposits
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('month',now()) - interval '{{Days}} days'
        ),
        cte_imx_withdrawals as (
                select 
                        date_trunc('month', "timestamp") as day 
                        , "sender" as address
                        , 'withdrawals' as tx_type
                from imx_withdrawals  
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('month',now()) - interval '{{Days}} days'
        ),
        cte_imx_orders as (
                select 
                        date_trunc('month', "updated_timestamp") as day 
                        , "user" as address
                        , 'orders' as tx_type
                from imx_orders   
                WHERE updated_timestamp < date_trunc('day', now())
                        AND updated_timestamp >= date_trunc('month',now()) - interval '{{Days}} days'
        ),
        cte_imx_transfers as (
                select 
                        date_trunc('month', "timestamp") as day
                        , "user" as address
                        , 'transfers' as tx_type
                from imx_transfers
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('month',now()) - interval '{{Days}} days'
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

        ## count of all addresses that actively interacted on imx (not mints, because they are not triggered by users themselves). Only fullfilled orders are counted as well.
    ,'imx_aa_last30d': """
        with date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
                ),

        cte_imx_deposits as (
        		SELECT 
				    d.day, 
				    "user" as address
				FROM date_range d
				LEFT JOIN 
				    imx_deposits b ON b.timestamp >= d.day - INTERVAL '30 days' AND b.timestamp <= d.day
				GROUP BY 1,2
        ),
        cte_imx_withdrawals as (
        		SELECT 
				    d.day, 
				    "sender" as address
				FROM date_range d
				LEFT JOIN 
				    imx_withdrawals b ON b.timestamp >= d.day - INTERVAL '30 days' AND b.timestamp <= d.day
				GROUP BY 1,2
        ),
        cte_imx_orders as (
        		SELECT 
				    d.day, 
				    "user" as address
				FROM date_range d
				LEFT JOIN 
				    imx_orders b ON b.updated_timestamp >= d.day - INTERVAL '30 days' AND b.updated_timestamp <= d.day
				GROUP BY 1,2
        ),
        cte_imx_transfers as (
        		SELECT 
				    d.day, 
				    "user" as address
				FROM date_range d
				LEFT JOIN 
				    imx_transfers b ON b.timestamp >= d.day - INTERVAL '30 days' AND b.timestamp <= d.day
				GROUP BY 1,2
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

        ,'arbitrum_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM arbitrum_tx tx
        WHERE
                tx.gas_used > 0
                AND block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1

        """

        ,'arbitrum_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                arbitrum_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1

        """

        ### OP Mainnet
        ,'optimism_txcount_raw': """
        SELECT date_trunc('day', ot.block_timestamp) AS day,
                count(*) AS value,
                'optimism' AS origin_key
        FROM optimism_tx ot
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                and gas_price > 0
        GROUP BY (date_trunc('day', ot.block_timestamp))
        """

        ,'optimism_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM optimism_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1

        """

        ,'optimism_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                optimism_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
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

        ,'base_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM base_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'base_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                base_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
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

        ,'polygon_zkevm_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM polygon_zkevm_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'polygon_zkevm_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                polygon_zkevm_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
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

        ,'zksync_era_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM zksync_era_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'zksync_era_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                zksync_era_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
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

        ,'zora_fees_paid_eth': """
        with zora_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.zora_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee AS value
        FROM zora_tx_filtered z
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

        ,'zora_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM public.zora_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'zora_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                zora_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
        """

        ,'zora_txcosts_median_eth': """
        WITH
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
                z.median_tx_fee as value
        FROM zora_median z
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


        ,'pgn_fees_paid_eth': """
        with pgn_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.gitcoin_pgn_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                pgn.day,
                pgn.total_tx_fee AS value
        FROM pgn_tx_filtered pgn
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

        ,'pgn_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM gitcoin_pgn_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

         ,'pgn_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                gitcoin_pgn_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
        """


    ,'pgn_txcosts_median_eth': """
        WITH 
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
                pgn.median_tx_fee as value
        FROM pgn_median pgn
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

    ,'linea_fees_paid_eth': """
        WITH linea_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.linea_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee AS value
        FROM linea_tx_filtered z
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

        ,'linea_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM linea_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'linea_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                linea_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
        """

    ,'linea_txcosts_median_eth': """
        WITH 
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
                z.median_tx_fee as value
        FROM linea_median z
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

        ,'mantle_fees_paid_eth': """
        WITH mnt_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'MNT' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
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
                mantle.total_tx_fee * e.price_usd / eth.price_usd AS value
        FROM mantle_tx_filtered mantle
        LEFT JOIN mnt_price e ON mantle.day = e."date"
        LEFT JOIN eth_price eth ON mantle.day = eth."date"
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
        ,'mantle_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM mantle_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'mantle_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                mantle_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
        """

        ,'mantle_txcosts_median_eth': """
        WITH mnt_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'MNT' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", price_usd
                FROM public.prices_daily
                WHERE token_symbol = 'ETH' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
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
                mantle.median_tx_fee * e.price_usd / eth.price_usd as value
        FROM mantle_median mantle
        LEFT JOIN mnt_price e ON mantle.day = e."date"
        LEFT JOIN eth_price eth ON mantle.day = eth."date"
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

        ,'scroll_fees_paid_eth': """
        WITH scroll_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.scroll_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee AS value
        FROM scroll_tx_filtered z
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

        ,'scroll_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM scroll_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'scroll_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                scroll_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
        """


    ,'scroll_txcosts_median_eth': """
        WITH 
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
                z.median_tx_fee as value
        FROM scroll_median z
        ORDER BY z.day DESC
   """
        # Loopring
        ,'loopring_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value,
                'loopring' AS origin_key
        FROM loopring_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY (date_trunc('day', st.block_timestamp))
        """

        ,'loopring_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.loopring_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
        """

        ,'loopring_daa': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(distinct from_address) AS value 
        FROM public.loopring_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        order by 1 DESC
        """

        ,'loopring_maa': """
        SELECT 
                date_trunc('month', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM loopring_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('month', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'loopring_aa_last30d': """
        WITH date_range AS (
                SELECT generate_series(
                        current_date - INTERVAL '{{Days}} days', 
                        current_date - INTERVAL '{{Days_Start}} days', 
                        '1 day'::INTERVAL
                )::DATE AS day
        )
        SELECT 
                d.day, 
                COUNT(DISTINCT b.from_address) AS value
        FROM date_range d
        LEFT JOIN 
                loopring_tx b ON b.block_timestamp >= d.day - INTERVAL '30 days' AND b.block_timestamp <= d.day
        GROUP BY 1
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
        if query_parameters is not None:
                self.sql = self.replace_query_parameters(self.sql_raw, query_parameters)
        else:
                self.sql = self.sql_raw
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.last_token = None
        self.last_execution_loaded = None

sql_queries = [
    ## Multichain
    #SQLQuery(metric_key = "profit_eth", origin_key = "multi", sql=sql_q["profit_eth"], query_parameters={"Days": 7})
    # ,SQLQuery(metric_key = "user_base_daily", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7, "aggregation": "day"})
    SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4, "aggregation": "week"})
    # ,SQLQuery(metric_key = "user_base_monthly", origin_key = "multi", sql=sql_q["user_base_xxx"], query_parameters={"Days": 7*4*12, "aggregation": "month"})


    ## IMX
    ,SQLQuery(metric_key = "txcount", origin_key = "imx", sql=sql_q["imx_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "imx", sql=sql_q["imx_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "imx", sql=sql_q["imx_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "imx", sql=sql_q["imx_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    #,SQLQuery(metric_key = "new_addresses", origin_key = "imx", sql=sql_q["ethereum_new_addresses"], query_parameters={"Days": 7})
    #,SQLQuery(metric_key = "fees_paid_usd", origin_key = "imx", sql=sql_q["imx_fees_paid_usd"], query_parameters={"Days": 7})

    ## Arbitrum
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum", sql=sql_q["arbitrum_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "maa", origin_key = "arbitrum", sql=sql_q["arbitrum_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum", sql=sql_q["arbitrum_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})

    ## OP Mainnet
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "optimism", sql=sql_q["optimism_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "maa", origin_key = "optimism", sql=sql_q["optimism_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "optimism", sql=sql_q["optimism_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})

    ## Base
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "base", sql=sql_q["base_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "maa", origin_key = "base", sql=sql_q["base_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "base", sql=sql_q["base_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})

    ## Polygon zkEVM
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "maa", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})

    ## zkSync Era
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "zksync_era", sql=sql_q["zksync_era_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "maa", origin_key = "zksync_era", sql=sql_q["zksync_era_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "zksync_era", sql=sql_q["zksync_era_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})

    ## Zora
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "zora", sql=sql_q["zora_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "zora", sql=sql_q["zora_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "zora", sql=sql_q["zora_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "zora", sql=sql_q["zora_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "zora", sql=sql_q["zora_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "zora", sql=sql_q["zora_fees_paid_eth"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "zora", sql=sql_q["zora_txcosts_median_eth"], query_parameters={"Days": 7})

    ## PGN
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_paid_eth"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcosts_median_eth"], query_parameters={"Days": 7})
    
    ## Linea
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "linea", sql=sql_q["linea_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "linea", sql=sql_q["linea_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "linea", sql=sql_q["linea_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "linea", sql=sql_q["linea_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "linea", sql=sql_q["linea_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "linea", sql=sql_q["linea_fees_paid_eth"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "linea", sql=sql_q["linea_txcosts_median_eth"], query_parameters={"Days": 7})

    ## Mantle
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "mantle", sql=sql_q["mantle_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "mantle", sql=sql_q["mantle_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "mantle", sql=sql_q["mantle_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "mantle", sql=sql_q["mantle_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "mantle", sql=sql_q["mantle_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", sql=sql_q["mantle_fees_paid_eth"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", sql=sql_q["mantle_txcosts_median_eth"], query_parameters={"Days": 7})
    
    ## Scroll
    ,SQLQuery(metric_key = "txcount_raw", origin_key = "scroll", sql=sql_q["scroll_txcount_raw"], query_parameters={"Days": 30})
    ,SQLQuery(metric_key = "txcount", origin_key = "scroll", sql=sql_q["scroll_txcount"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "daa", origin_key = "scroll", sql=sql_q["scroll_daa"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "maa", origin_key = "scroll", sql=sql_q["scroll_maa"], query_parameters={"Days": 60})
    ,SQLQuery(metric_key = "aa_last30d", origin_key = "scroll", sql=sql_q["scroll_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
    ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "scroll", sql=sql_q["scroll_fees_paid_eth"], query_parameters={"Days": 7})
    ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "scroll", sql=sql_q["scroll_txcosts_median_eth"], query_parameters={"Days": 7})
   

   ## Loopring
   ,SQLQuery(metric_key = "txcount_raw", origin_key = "loopring", sql=sql_q["loopring_txcount_raw"], query_parameters={"Days": 30})
   ,SQLQuery(metric_key = "txcount", origin_key = "loopring", sql=sql_q["loopring_txcount"], query_parameters={"Days": 7})
   ,SQLQuery(metric_key = "daa", origin_key = "loopring", sql=sql_q["loopring_daa"], query_parameters={"Days": 7})
   ,SQLQuery(metric_key = "maa", origin_key = "loopring", sql=sql_q["loopring_maa"], query_parameters={"Days": 60})
   ,SQLQuery(metric_key = "aa_last30d", origin_key = "loopring", sql=sql_q["loopring_aa_last30d"], query_parameters={"Days": 3, "Days_Start": 1})
]