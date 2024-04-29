def get_cross_chain_activity(origin_key):
    query = f"""
        with excl_chain as (
            select 
                address,
                origin_key 
            from fact_active_addresses faa 
            where faa."date" between current_date - interval '7 days' and current_date
                and origin_key <> '{origin_key}'
        )

        , tmp as (
            SELECT 
                    aa.address AS address,
                    CASE WHEN count(distinct ex.origin_key) > 1 THEN 'multiple' ELSE MAX(ex.origin_key) END as cca 
            FROM fact_active_addresses aa
            left join excl_chain ex on aa.address = ex.address
            WHERE aa."date" between current_date - interval '7 days' and current_date
                and aa.origin_key = '{origin_key}'
            group by 1
        )

        select 
            coalesce(
                'cca_last7d_' || cca,
                'cca_last7d_exclusive'
            ) as metric_key,
            (current_date - interval '1 days')::DATE as day,
            Count(*) as value
        from tmp
        group by 1,2
    """
    return query

sql_q= {
        'user_base_xxx': """
                with chain_info as (
                        SELECT 
                                DATE_TRUNC('{{aggregation}}', date) AS day,
                                address,
                                CASE WHEN count(distinct origin_key) > 1 THEN 'multiple' ELSE MAX(origin_key) END as origin_key
                        FROM fact_active_addresses
                        WHERE
                                date < DATE_TRUNC('{{aggregation}}', NOW())
                                AND date >= DATE_TRUNC('{{aggregation}}', NOW() - INTERVAL  '{{Days}} days')
                        GROUP BY 1,2
                )

                SELECT
                        day,
                        origin_key,
                        COUNT(DISTINCT address) AS val
                FROM
                        chain_info
                GROUP BY 1,2 
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
        ),	
        cte_imx_mints as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'mints' as tx_type
                from imx_mints
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
        ),    
        cte_imx_trades as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'trades' as tx_type
                from imx_trades
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
        ),    
        cte_imx_transfers as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'transfers' as tx_type
                from imx_transfers
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
                group by 1
        ),
        cte_imx_withdrawals as (
                select 
                        date_trunc('day', "timestamp") as day, Count(*) as value, 'withdrawals' as tx_type
                from imx_withdrawals  
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('day',now()) - interval '{{Days}} days'
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
                select * from cte_imx_transfers 
        )
        select 
                day, SUM(value) as val 
        from unioned 
        group by 1
    """
    
    ## count of all addresses that activels interacted on imx (not mints, because they are not triggered by users themselves). Only fullfilled orders are counted as well.
    ,'imx_aa_xxx': """
         with 
        cte_imx_deposits as (
                select 
                        date_trunc('{{aggregation}}', "timestamp") as day 
                        , "user" as address
                        , 'deposits' as tx_type
                from imx_deposits
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('{{aggregation}}',now() - interval '{{Days}} days')
        ),
        cte_imx_withdrawals as (
                select 
                        date_trunc('{{aggregation}}', "timestamp") as day 
                        , "sender" as address
                        , 'withdrawals' as tx_type
                from imx_withdrawals  
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('{{aggregation}}',now() - interval '{{Days}} days')
        ),
        cte_imx_orders as (
                select 
                        date_trunc('{{aggregation}}', "updated_timestamp") as day 
                        , "user" as address
                        , 'orders' as tx_type
                from imx_orders   
                WHERE updated_timestamp < date_trunc('day', now())
                        AND updated_timestamp >= date_trunc('{{aggregation}}',now() - interval '{{Days}} days')
        ),
        cte_imx_transfers as (
                select 
                        date_trunc('{{aggregation}}', "timestamp") as day
                        , "user" as address
                        , 'transfers' as tx_type
                from imx_transfers
                WHERE timestamp < date_trunc('day', now())
                        AND timestamp >= date_trunc('{{aggregation}}',now() - interval '{{Days}} days')
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
    """

        ## count of all addresses that actively interacted on imx (not mints, because they are not triggered by users themselves). Only fullfilled orders are counted as well.
    ,'imx_aa_last_xxd': """
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
				    imx_deposits b ON b.timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.timestamp <= d.day + INTERVAL '1 days'
				GROUP BY 1,2
        ),
        cte_imx_withdrawals as (
        		SELECT 
				    d.day, 
				    "sender" as address
				FROM date_range d
				LEFT JOIN 
				    imx_withdrawals b ON b.timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.timestamp <= d.day + INTERVAL '1 days'
				GROUP BY 1,2
        ),
        cte_imx_orders as (
        		SELECT 
				    d.day, 
				    "user" as address
				FROM date_range d
				LEFT JOIN 
				    imx_orders b ON b.updated_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.updated_timestamp <= d.day + INTERVAL '1 days'
				GROUP BY 1,2
        ),
        cte_imx_transfers as (
        		SELECT 
				    d.day, 
				    "user" as address
				FROM date_range d
				LEFT JOIN 
				    imx_transfers b ON b.timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.timestamp <= d.day + INTERVAL '1 days'
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
    """


        ### Arbitrum
        ,'arbitrum_txcount_raw': """
        SELECT date_trunc('day', at2.block_timestamp) AS day,
                count(*) AS value
        FROM arbitrum_tx at2
        WHERE at2.gas_used > 0
                AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'arbitrum_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM arbitrum_tx tx
        WHERE
                tx.gas_used > 0
                AND block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1

        """

        ,'arbitrum_aa_last_xxd': """
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
                arbitrum_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'arbitrum_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    arbitrum_tx
        WHERE   gas_used > 0
                AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### OP Mainnet
        ,'optimism_txcount_raw': """
        SELECT  date_trunc('day', ot.block_timestamp) AS day,
                count(*) AS value
        FROM    optimism_tx ot
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                and gas_price > 0
        GROUP BY 1
        """

        ,'optimism_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM optimism_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'optimism_aa_last_xxd': """
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
                optimism_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'optimism_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    optimism_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Base
        ,'base_txcount_raw': """
        SELECT  date_trunc('day', bt.block_timestamp) AS day,
                count(*) AS value
        FROM    base_tx bt
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'base_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM base_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'base_aa_last_xxd': """
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
                base_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'base_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    base_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """


        ### Polygon zkEVM
        ,'polygon_zkevm_txcount': """
        SELECT  date_trunc('day', pzt.block_timestamp) AS day,
                count(*) AS value
        FROM polygon_zkevm_tx pzt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'polygon_zkevm_txcount_raw': """
        SELECT date_trunc('day', pzt.block_timestamp) AS day,
                count(*) AS value
        FROM polygon_zkevm_tx pzt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'polygon_zkevm_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM polygon_zkevm_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'polygon_zkevm_aa_last_xxd': """
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
                polygon_zkevm_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'polygon_zkevm_txcosts_median_eth': """
        WITH 
        polygon_zkevm_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.polygon_zkevm_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.median_tx_fee as value
        FROM polygon_zkevm_median z
        """

        ,'polygon_zkevm_fees_paid_eth': """
        with polygon_zkevm_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.polygon_zkevm_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                z.day,
                z.total_tx_fee AS value
        FROM polygon_zkevm_filtered z
        """

        ,'polygon_zkevm_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    polygon_zkevm_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """
        
        ### zkSync Era
        ,'zksync_era_txcount_raw': """
        SELECT  date_trunc('day', zet.block_timestamp) AS day,
                count(*) AS value
        FROM zksync_era_tx zet
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'zksync_era_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM zksync_era_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'zksync_era_aa_last_xxd': """
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
                zksync_era_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'zksync_era_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    zksync_era_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Zora
        ,'zora_txcount_raw': """
        SELECT date_trunc('day', zt.block_timestamp) AS day,
                count(*) AS value
        FROM zora_tx zt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
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
        """

        ,'zora_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.zora_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'zora_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM public.zora_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'zora_aa_last_xxd': """
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
                zora_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
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
        """

        ,'zora_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    zora_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### PGN
        ,'pgn_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value
        FROM gitcoin_pgn_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
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
        """

        ,'pgn_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.gitcoin_pgn_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'pgn_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM gitcoin_pgn_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

         ,'pgn_aa_last_xxd': """
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
                gitcoin_pgn_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
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
        """

        ,'pgn_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    gitcoin_pgn_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Linea
        ,'linea_txcount_raw': """
        SELECT date_trunc('day', linea_tx.block_timestamp) AS day,
                count(*) AS value
        FROM linea_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'linea_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.linea_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
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
        """

        ,'linea_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM linea_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'linea_aa_last_xxd': """
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
                linea_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
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
        """

        ,'linea_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    linea_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Mantle
        ,'mantle_txcount_raw': """
        SELECT date_trunc('day', mt.block_timestamp) AS day,
                count(*) AS value
        FROM mantle_tx mt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'mantle_fees_paid_eth': """
        WITH mnt_price AS (
                SELECT "date", value as price_usd 
                FROM fact_kpis
                WHERE origin_key  = 'mantle' and metric_key = 'price_usd' 
                AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", value as price_usd 
                FROM fact_kpis
                WHERE origin_key  = 'ethereum' and metric_key = 'price_usd' 
                AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
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
        """

        ,'mantle_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.mantle_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'mantle_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM mantle_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'mantle_aa_last_xxd': """
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
                mantle_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'mantle_txcosts_median_eth': """
        WITH mnt_price AS (
                SELECT "date", value as price_usd 
                FROM fact_kpis
                WHERE origin_key  = 'mantle' and metric_key = 'price_usd' 
                AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", value as price_usd 
                FROM fact_kpis
                WHERE origin_key  = 'ethereum' and metric_key = 'price_usd' 
                AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
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
        """

        ,'mantle_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    mantle_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        # Scroll
        ,'scroll_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value
        FROM scroll_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'scroll_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.scroll_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
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
        """

        ,'scroll_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM scroll_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'scroll_aa_last_xxd': """
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
                scroll_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
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
        """

        ,'scroll_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    scroll_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        # Loopring
        ,'loopring_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value
        FROM loopring_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'loopring_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.loopring_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'loopring_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM loopring_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'loopring_aa_last_xxd': """
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
                loopring_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        # Rhino
        ,'rhino_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value
        FROM rhino_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'rhino_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.rhino_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'rhino_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM rhino_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'rhino_aa_last_xxd': """
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
                rhino_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        # Starknet
        ,'starknet_txcount_raw': """
        SELECT date_trunc('day', st.block_timestamp) AS day,
                count(*) AS value
        FROM starknet_tx st
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'starknet_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.starknet_tx
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """
        
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

        ,'starknet_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM starknet_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'starknet_aa_last_xxd': """
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
                starknet_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
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

        ### Metis
        ,'metis_txcount_raw': """
        SELECT date_trunc('day', mt.block_timestamp) AS day,
                count(*) AS value
        FROM metis_tx mt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'metis_fees_paid_eth': """
        WITH token_price AS (
                SELECT "date", value as price_usd
                FROM public.fact_kpis
                WHERE origin_key = 'metis' and metric_key = 'price_usd' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", value as price_usd
                FROM public.fact_kpis
                WHERE origin_key = 'ethereum' and metric_key = 'price_usd' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        metis_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.metis_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                metis.day,
                metis.total_tx_fee * e.price_usd / eth.price_usd AS value
        FROM metis_tx_filtered metis
        LEFT JOIN token_price e ON metis.day = e."date"
        LEFT JOIN eth_price eth ON metis.day = eth."date"
        """

        ,'metis_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.metis_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                AND gas_price > 0
        GROUP BY 1
        """

        ,'metis_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM metis_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

        ,'metis_aa_last_xxd': """
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
                metis_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'metis_txcosts_median_eth': """
        WITH token_price AS (
                SELECT "date", value as price_usd
                FROM public.fact_kpis
                WHERE origin_key = 'metis' and metric_key = 'price_usd' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        eth_price AS (
                SELECT "date", value as price_usd
                FROM public.fact_kpis
                WHERE origin_key = 'ethereum' and metric_key = 'price_usd' AND "date" BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        ),
        metis_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.metis_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                metis.day,
                metis.median_tx_fee * e.price_usd / eth.price_usd as value
        FROM metis_median metis
        LEFT JOIN token_price e ON metis.day = e."date"
        LEFT JOIN eth_price eth ON metis.day = eth."date"
        """

        ,'metis_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    metis_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Blast
        ,'blast_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value
        FROM blast_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        AND gas_price > 0
        GROUP BY 1
        """

        ,'blast_fees_paid_eth': """
        with blast_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.blast_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                blast.day,
                blast.total_tx_fee AS value
        FROM blast_tx_filtered blast
        """

        ,'blast_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.blast_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'blast_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM blast_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

         ,'blast_aa_last_xxd': """
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
                blast_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'blast_txcosts_median_eth': """
        WITH 
        blast_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.blast_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                blast.day,
                blast.median_tx_fee as value
        FROM blast_median blast
        """

        ,'blast_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    blast_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Manta
        ,'manta_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value
        FROM manta_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        AND gas_price > 0
        GROUP BY 1
        """

        ,'manta_fees_paid_eth': """
        with manta_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.manta_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                manta.day,
                manta.total_tx_fee AS value
        FROM manta_tx_filtered manta
        """

        ,'manta_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.manta_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'manta_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM manta_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

         ,'manta_aa_last_xxd': """
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
                manta_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """


        ,'manta_txcosts_median_eth': """
        WITH 
        manta_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.manta_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                manta.day,
                manta.median_tx_fee as value
        FROM manta_median manta
        """

        ,'manta_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    manta_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

         ### Ethereum
        ,'ethereum_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value
        FROM ethereum_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ### Mode
        ,'mode_txcount_raw': """
        SELECT date_trunc('day', gpt.block_timestamp) AS day,
                count(*) AS value
        FROM mode_tx gpt
        WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        AND gas_price > 0
        GROUP BY 1
        """

        ,'mode_fees_paid_eth': """
        with mode_tx_filtered AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        SUM(tx_fee) AS total_tx_fee
                FROM public.mode_tx
                WHERE block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                mode.day,
                mode.total_tx_fee AS value
        FROM mode_tx_filtered mode
        """

        ,'mode_txcount': """
        SELECT 
                DATE_TRUNC('day', block_timestamp) AS day,
                COUNT(*) AS value
        FROM public.mode_tx
        WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
        GROUP BY 1
        """

        ,'mode_aa_xxx': """
        SELECT 
                date_trunc('{{aggregation}}', tx.block_timestamp) AS day,
                count(DISTINCT from_address) as value
        FROM mode_tx tx
        WHERE
                block_timestamp < date_trunc('day', current_date)
                AND block_timestamp >= date_trunc('{{aggregation}}', current_date - interval '{{Days}}' day)
        GROUP BY  1
        """

         ,'mode_aa_last_xxd': """
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
                mode_tx b ON b.block_timestamp >= d.day - INTERVAL '{{Timerange}} days' AND b.block_timestamp <= d.day + INTERVAL '1 days'
        GROUP BY 1
        """

        ,'mode_txcosts_median_eth': """
        WITH 
        mode_median AS (
                SELECT
                        date_trunc('day', "block_timestamp") AS day,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                FROM public.mode_tx
                WHERE gas_price <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
                GROUP BY 1
        )
        SELECT
                mode.day,
                mode.median_tx_fee as value
        FROM mode_median mode
        """

        ,'mode_gas_per_second': """
        SELECT  date_trunc('day', block_timestamp) AS day,
                sum(gas_used) / (24*60*60) AS value
        FROM    mode_tx
        WHERE   block_timestamp BETWEEN date_trunc('day', now()) - interval '{{Days}} days' AND date_trunc('day', now())
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
    def __init__(self, sql: str, metric_key: str, origin_key: str, currency_dependent:bool = True, query_parameters: dict = None):
        self.sql_raw = sql 
        if query_parameters is not None:
                self.sql = self.replace_query_parameters(self.sql_raw, query_parameters)
        else:
                self.sql = self.sql_raw
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.currency_dependent = currency_dependent ## if false, the query can in parellel to the currency queries 
        self.last_token = None
        self.last_execution_loaded = None

sql_queries = [
    ## Multichain
        #SQLQuery(metric_key = "profit_eth", origin_key = "multi", sql=sql_q["profit_eth"], query_parameters={"Days": 7})
        # ,SQLQuery(metric_key = "user_base_daily", origin_key = "multi", sql=sql_q["user_base_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", sql=sql_q["user_base_xxx"], currency_dependent = False, query_parameters={"Days": 7*4, "aggregation": "week"})
        # ,SQLQuery(metric_key = "user_base_monthly", origin_key = "multi", sql=sql_q["user_base_xxx"], currency_dependent = False, query_parameters={"Days": 7*4*12, "aggregation": "month"})

        ## IMX
        ,SQLQuery(metric_key = "txcount", origin_key = "imx", sql=sql_q["imx_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "imx", sql=sql_q["imx_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "imx", sql=sql_q["imx_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "imx", sql=sql_q["imx_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "imx", sql=sql_q["imx_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "imx", sql=sql_q["imx_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        #,SQLQuery(metric_key = "new_addresses", origin_key = "imx", sql=sql_q["ethereum_new_addresses"], currency_dependent = False, query_parameters={"Days": 7})
        #,SQLQuery(metric_key = "fees_paid_usd", origin_key = "imx", sql=sql_q["imx_fees_paid_usd"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "imx", sql=get_cross_chain_activity('imx'), currency_dependent = False, query_parameters={})

        ## Arbitrum
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum", sql=sql_q["arbitrum_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        #,SQLQuery(metric_key = "waa", origin_key = "arbitrum", sql=sql_q["arbitrum_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum", sql=sql_q["arbitrum_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum", sql=sql_q["arbitrum_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum", sql=sql_q["arbitrum_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "arbitrum", sql=get_cross_chain_activity('arbitrum'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum", sql=sql_q["arbitrum_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## OP Mainnet
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "optimism", sql=sql_q["optimism_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        #,SQLQuery(metric_key = "waa", origin_key = "optimism", sql=sql_q["optimism_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "optimism", sql=sql_q["optimism_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "optimism", sql=sql_q["optimism_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "optimism", sql=sql_q["optimism_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "optimism", sql=get_cross_chain_activity('optimism'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "optimism", sql=sql_q["optimism_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Base
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "base", sql=sql_q["base_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        #,SQLQuery(metric_key = "waa", origin_key = "base", sql=sql_q["base_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "base", sql=sql_q["base_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "base", sql=sql_q["base_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "base", sql=sql_q["base_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "base", sql=get_cross_chain_activity('base'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "base", sql=sql_q["base_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## zkSync Era
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "zksync_era", sql=sql_q["zksync_era_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        #,SQLQuery(metric_key = "waa", origin_key = "zksync_era", sql=sql_q["zksync_era_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "zksync_era", sql=sql_q["zksync_era_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "zksync_era", sql=sql_q["zksync_era_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "zksync_era", sql=sql_q["zksync_era_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "zksync_era", sql=get_cross_chain_activity('zksync_era'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "zksync_era", sql=sql_q["zksync_era_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Polygon zkEVM
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_txcount"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "daa", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "polygon_zkevm", sql=get_cross_chain_activity('polygon_zkevm'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "polygon_zkevm", sql=sql_q["polygon_zkevm_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Zora
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "zora", sql=sql_q["zora_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "zora", sql=sql_q["zora_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "zora", sql=sql_q["zora_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "zora", sql=sql_q["zora_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "zora", sql=sql_q["zora_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "zora", sql=sql_q["zora_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "zora", sql=sql_q["zora_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "zora", sql=sql_q["zora_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "zora", sql=sql_q["zora_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "zora", sql=get_cross_chain_activity('zora'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "zora", sql=sql_q["zora_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## PGN
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "gitcoin_pgn", sql=sql_q["pgn_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gitcoin_pgn", sql=sql_q["pgn_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gitcoin_pgn", sql=sql_q["pgn_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "gitcoin_pgn", sql=get_cross_chain_activity('gitcoin_pgn'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "gitcoin_pgn", sql=sql_q["pgn_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})
    
        ## Linea
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "linea", sql=sql_q["linea_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "linea", sql=sql_q["linea_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "linea", sql=sql_q["linea_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "linea", sql=sql_q["linea_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "linea", sql=sql_q["linea_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "linea", sql=sql_q["linea_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "linea", sql=sql_q["linea_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "linea", sql=sql_q["linea_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "linea", sql=sql_q["linea_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "linea", sql=get_cross_chain_activity('linea'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "linea", sql=sql_q["linea_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Mantle
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "mantle", sql=sql_q["mantle_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "mantle", sql=sql_q["mantle_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "mantle", sql=sql_q["mantle_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "mantle", sql=sql_q["mantle_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "mantle", sql=sql_q["mantle_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mantle", sql=sql_q["mantle_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mantle", sql=sql_q["mantle_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", sql=sql_q["mantle_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", sql=sql_q["mantle_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "mantle", sql=get_cross_chain_activity('mantle'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mantle", sql=sql_q["mantle_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})
    
        ## Scroll
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "scroll", sql=sql_q["scroll_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "scroll", sql=sql_q["scroll_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "scroll", sql=sql_q["scroll_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "scroll", sql=sql_q["scroll_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "scroll", sql=sql_q["scroll_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "scroll", sql=sql_q["scroll_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "scroll", sql=sql_q["scroll_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "scroll", sql=sql_q["scroll_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "scroll", sql=sql_q["scroll_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "scroll", sql=get_cross_chain_activity('scroll'), currency_dependent = False, query_parameters={})   
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "scroll", sql=sql_q["scroll_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Loopring
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "loopring", sql=sql_q["loopring_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "loopring", sql=sql_q["loopring_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "loopring", sql=sql_q["loopring_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "loopring", sql=sql_q["loopring_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "loopring", sql=sql_q["loopring_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "loopring", sql=sql_q["loopring_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "loopring", sql=sql_q["loopring_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "loopring", sql=get_cross_chain_activity('loopring'), currency_dependent = False, query_parameters={})

        ## Rhino
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "rhino", sql=sql_q["rhino_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "rhino", sql=sql_q["rhino_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "rhino", sql=sql_q["rhino_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "rhino", sql=sql_q["rhino_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "rhino", sql=sql_q["rhino_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "rhino", sql=sql_q["rhino_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "rhino", sql=sql_q["rhino_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "cca", origin_key = "rhino", sql=get_cross_chain_activity('rhino'), currency_dependent = False, query_parameters={})

        ## Starknet
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "starknet", sql=sql_q["starknet_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "starknet", sql=sql_q["starknet_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "user_base_weekly", origin_key = "starknet", sql=sql_q["starknet_user_base_xxx"], currency_dependent = False, query_parameters={"Days": 28, "aggregation": "week"})
        ,SQLQuery(metric_key = "daa", origin_key = "starknet", sql=sql_q["starknet_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "starknet", sql=sql_q["starknet_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "starknet", sql=sql_q["starknet_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "starknet", sql=sql_q["starknet_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "starknet", sql=sql_q["starknet_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "starknet", sql=sql_q["starknet_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "starknet", sql=sql_q["starknet_txcosts_median_eth"], query_parameters={"Days": 7})

        ## Metis
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "metis", sql=sql_q["metis_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "metis", sql=sql_q["metis_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "metis", sql=sql_q["metis_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "metis", sql=sql_q["metis_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "metis", sql=sql_q["metis_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "metis", sql=sql_q["metis_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "metis", sql=sql_q["metis_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "metis", sql=sql_q["metis_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "metis", sql=sql_q["metis_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "metis", sql=get_cross_chain_activity('metis'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "metis", sql=sql_q["metis_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Blast
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "blast", sql=sql_q["blast_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "blast", sql=sql_q["blast_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "blast", sql=sql_q["blast_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "blast", sql=sql_q["blast_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "blast", sql=sql_q["blast_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "blast", sql=sql_q["blast_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "blast", sql=sql_q["blast_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "blast", sql=sql_q["blast_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "blast", sql=sql_q["blast_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "blast", sql=get_cross_chain_activity('blast'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "blast", sql=sql_q["blast_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Manta
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "manta", sql=sql_q["manta_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "manta", sql=sql_q["manta_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "manta", sql=sql_q["manta_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "manta", sql=sql_q["manta_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "manta", sql=sql_q["manta_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "manta", sql=sql_q["manta_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "manta", sql=sql_q["manta_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "manta", sql=sql_q["manta_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "manta", sql=sql_q["manta_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "manta", sql=get_cross_chain_activity('manta'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "manta", sql=sql_q["manta_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})

        ## Ethereum
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "ethereum", sql=sql_q["ethereum_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})

        ## Mode
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "mode", sql=sql_q["mode_txcount_raw"], currency_dependent = False, query_parameters={"Days": 30})
        ,SQLQuery(metric_key = "txcount", origin_key = "mode", sql=sql_q["mode_txcount"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "daa", origin_key = "mode", sql=sql_q["mode_aa_xxx"], currency_dependent = False, query_parameters={"Days": 7, "aggregation": "day"})
        #,SQLQuery(metric_key = "waa", origin_key = "mode", sql=sql_q["mode_aa_xxx"], currency_dependent = False, query_parameters={"Days": 21, "aggregation": "week"})
        ,SQLQuery(metric_key = "maa", origin_key = "mode", sql=sql_q["mode_aa_xxx"], currency_dependent = False, query_parameters={"Days": 60, "aggregation": "month"})
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mode", sql=sql_q["mode_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 6})
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mode", sql=sql_q["mode_aa_last_xxd"], currency_dependent = False, query_parameters={"Days": 3, "Days_Start": 1, "Timerange" : 29})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mode", sql=sql_q["mode_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mode", sql=sql_q["mode_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "cca", origin_key = "mode", sql=get_cross_chain_activity('mode'), currency_dependent = False, query_parameters={})
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mode", sql=sql_q["mode_gas_per_second"], currency_dependent = False, query_parameters={"Days": 7})
]