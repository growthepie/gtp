
## Dict that stores all SQL code for different queries. Parameters are passed in as {{parameter_name}} and are replaced in the code below.

flipside_sql = {
    ##################### Layer 1s #####################
    # Ethereum aggregations
    'ethereum_txcount' : 
    """
        SELECT 
            DATE_TRUNC('day', block_timestamp) AS day,
            count(*) as val
        FROM ethereum.core.fact_transactions
        WHERE
            block_timestamp::DATE >= dateadd(day, -{{Days}}, current_date())
            AND block_timestamp::DATE < DATE_TRUNC('day', CURRENT_DATE)
        GROUP BY
            1
        """
    ,'ethereum_daa' : 
    """
        SELECT 
            DATE_TRUNC('day', block_timestamp) AS day,
            count(DISTINCT from_address) as val
        FROM ethereum.core.fact_transactions
        WHERE
            block_timestamp::DATE >= dateadd(day, -{{Days}}, current_date())
            AND block_timestamp::DATE < DATE_TRUNC('day', CURRENT_DATE)
        GROUP BY
            1
        """
    ,'ethereum_waa' : 
    """
        SELECT 
            DATE_TRUNC('week', block_timestamp) AS day,
            count(DISTINCT from_address) as val
        FROM ethereum.core.fact_transactions
        WHERE
            block_timestamp::DATE >= dateadd(day, -{{Days}}, current_date())
            AND block_timestamp::DATE < DATE_TRUNC('week', CURRENT_DATE)
        GROUP BY
            1
        """
    ,'ethereum_maa' : 
    """
        SELECT 
            DATE_TRUNC('month', block_timestamp) AS day,
            count(DISTINCT from_address) as val
        FROM ethereum.core.fact_transactions
        WHERE
            block_timestamp::DATE >= dateadd(day, -{{Days}}, current_date())
            AND block_timestamp::DATE < DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY
            1
        """
    ,'ethereum_new_addresses' :
    """
        WITH first_tx as (
            SELECT 
                to_address as address
                , min(block_timestamp) as first_tx_ts 
            FROM ethereum.core.fact_transactions
            GROUP BY 1
        )

        SELECT 
            DATE_TRUNC('day', first_tx_ts) as day
            , count(distinct address) as val
        FROM  first_tx
        WHERE first_tx_ts < date_trunc('day', current_date)
            AND first_tx_ts >= dateadd(day, -{{Days}}, current_date())
        GROUP BY day
    """
    ,'ethereum_fees_paid_usd' :
    """
        WITH eth_price AS (
        SELECT
            HOUR,
            PRICE
        FROM ethereum.core.fact_hourly_token_prices
        WHERE
            SYMBOL = 'WETH'
        ORDER BY 1 DESC)

        SELECT
            date_trunc('day', BLOCK_TIMESTAMP) as day,
            SUM(TX_FEE * PRICE) AS val
        FROM ethereum.core.fact_transactions t
        LEFT JOIN eth_price e ON date_trunc('hour', t.BLOCK_TIMESTAMP) = e.hour
        WHERE
            day < CURRENT_DATE
            and day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        GROUP BY 1
        ORDER BY 1 DESC
    """

    # Stables on L1
    ,'imx_stables':
    """
        with stables as (
        SELECT token, contract_address, decimals 
        FROM (
            VALUES
            ('USDC', lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'), 6) -- USDC
            ,('USDT', lower('0xdac17f958d2ee523a2206206994597c13d831ec7'), 6) --USDT
            ,('DAI', lower('0x6b175474e89094c44da98b954eedeac495271d0f'), 18) --DAI 18 Decimals
            ,('BUSD', lower('0x4fabb145d64652a948d72533023f6e7a623c7c53'), 18) --BUSD 18 Decimals
            ,('TUSD', lower('0x0000000000085d4780b73119b644ae5ecd22b376'), 18) --TUSD 18 Decimals
            ,('FRAX', lower('0x853d955acef822db058eb8505911ed77f175b99e'), 18) -- FRAX 18 Decimals
            ,('USDP', lower('0x8e870d67f660d95d5be530380d0ec0bd388289e1'), 18) -- USDP 18 Decimals
            ,('GUSD', lower('0x056fd409e1d7a124bd7017459dfea2f387b6d5cd'), 2) -- GUSD 2 Decimals
            ) AS x (token, contract_address, decimals )
        )

        select 
        date_trunc('day', BLOCK_TIMESTAMP) AS day,
        AVG(balance / (pow(10, stables.decimals))) as val
        from ethereum.core.fact_token_balances fact
        inner join stables on fact.contract_address = stables.contract_address
        left join ethereum.core.fact_hourly_token_prices tp on 
        date_trunc('hour', fact.BLOCK_TIMESTAMP) = tp.hour
        and stables.contract_address = tp.token_address
        where user_address = lower('0x5FDCCA53617f4d2b9134B29090C87D01058e27e9')
        and day < CURRENT_DATE
        and day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        group by 1
    """
    ,'polygonzkevm_stables':
    """
        with stables as (
        SELECT token, contract_address, decimals 
        FROM (
            VALUES
            ('USDC', lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'), 6) -- USDC
            ,('USDT', lower('0xdac17f958d2ee523a2206206994597c13d831ec7'), 6) --USDT
            ,('DAI', lower('0x6b175474e89094c44da98b954eedeac495271d0f'), 18) --DAI 18 Decimals
            ,('BUSD', lower('0x4fabb145d64652a948d72533023f6e7a623c7c53'), 18) --BUSD 18 Decimals
            ,('TUSD', lower('0x0000000000085d4780b73119b644ae5ecd22b376'), 18) --TUSD 18 Decimals
            ,('FRAX', lower('0x853d955acef822db058eb8505911ed77f175b99e'), 18) -- FRAX 18 Decimals
            ,('USDP', lower('0x8e870d67f660d95d5be530380d0ec0bd388289e1'), 18) -- USDP 18 Decimals
            ,('GUSD', lower('0x056fd409e1d7a124bd7017459dfea2f387b6d5cd'), 2) -- GUSD 2 Decimals
            ) AS x (token, contract_address, decimals )
        )

        select 
        date_trunc('day', BLOCK_TIMESTAMP) AS day,
        AVG(balance / (pow(10, stables.decimals))) as val
        from ethereum.core.fact_token_balances fact
        inner join stables on fact.contract_address = stables.contract_address
        left join ethereum.core.fact_hourly_token_prices tp on 
        date_trunc('hour', fact.BLOCK_TIMESTAMP) = tp.hour
        and stables.contract_address = tp.token_address
        where user_address = lower('0x2a3DD3EB832aF982ec71669E178424b10Dca2EDe')
        and day < CURRENT_DATE
        and day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        group by 1
    """
    ,'zksyncera_stables':
    """
        with stables as (
        SELECT token, contract_address, decimals 
        FROM (
            VALUES
            ('USDC', lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'), 6) -- USDC
            ,('USDT', lower('0xdac17f958d2ee523a2206206994597c13d831ec7'), 6) --USDT
            ,('DAI', lower('0x6b175474e89094c44da98b954eedeac495271d0f'), 18) --DAI 18 Decimals
            ,('BUSD', lower('0x4fabb145d64652a948d72533023f6e7a623c7c53'), 18) --BUSD 18 Decimals
            ,('TUSD', lower('0x0000000000085d4780b73119b644ae5ecd22b376'), 18) --TUSD 18 Decimals
            ,('FRAX', lower('0x853d955acef822db058eb8505911ed77f175b99e'), 18) -- FRAX 18 Decimals
            ,('USDP', lower('0x8e870d67f660d95d5be530380d0ec0bd388289e1'), 18) -- USDP 18 Decimals
            ,('GUSD', lower('0x056fd409e1d7a124bd7017459dfea2f387b6d5cd'), 2) -- GUSD 2 Decimals
            ) AS x (token, contract_address, decimals )
        )

        select 
        date_trunc('day', BLOCK_TIMESTAMP) AS day,
        AVG(balance / (pow(10, stables.decimals))) as val
        from ethereum.core.fact_token_balances fact
        inner join stables on fact.contract_address = stables.contract_address
        left join ethereum.core.fact_hourly_token_prices tp on 
        date_trunc('hour', fact.BLOCK_TIMESTAMP) = tp.hour
        and stables.contract_address = tp.token_address
        where user_address = lower('0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063')
        and day < CURRENT_DATE
        and day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        group by 1
    """

    ##################### Layer 2s #####################
    # Arbitrum aggregations
    ,'arbitrum_txcount' :
    """
        SELECT 
            date_trunc('day', BLOCK_TIMESTAMP) AS day,
            count(*) AS val
        FROM arbitrum.core.fact_transactions
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x00000000000000000000000000000000000a4b05'
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'arbitrum_daa' :
    """
        SELECT 
            date_trunc('day', BLOCK_TIMESTAMP) AS day, 
            count(distinct "FROM_ADDRESS") AS val
        FROM arbitrum.core.fact_transactions
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x00000000000000000000000000000000000a4b05'
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'arbitrum_new_addresses' :
    """
        WITH first_tx AS (
        SELECT
            TO_ADDRESS AS address,
            min(BLOCK_TIMESTAMP) AS first_tx_ts 
        FROM arbitrum.core.fact_transactions
        WHERE TO_ADDRESS <> '0x00000000000000000000000000000000000a4b05'
        GROUP BY 1)

        SELECT 
            date_trunc('day', first_tx_ts) AS day,
            count(distinct address) AS val
        FROM first_tx
        WHERE 
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'arbitrum_fees_paid_usd' :
    """
        WITH eth_price AS (
        SELECT
            HOUR,
            PRICE
        FROM ethereum.core.fact_hourly_token_prices
        WHERE
            SYMBOL = 'WETH'
        ORDER BY 1 DESC)

        SELECT
            date_trunc('day', BLOCK_TIMESTAMP) AS day,
            SUM(TX_FEE * PRICE) AS val
        FROM arbitrum.core.fact_transactions t
        LEFT JOIN eth_price e ON date_trunc('hour', t.BLOCK_TIMESTAMP) = e.hour
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x00000000000000000000000000000000000a4b05'
        GROUP BY 1
        ORDER BY 1 DESC
    """
    # Arbitrum raw
    ,'arbitrum_transactions' : 
    """
        select 
            BLOCK_NUMBER, BLOCK_TIMESTAMP, BLOCK_HASH, TX_HASH, NONCE, POSITION, ORIGIN_FUNCTION_SIGNATURE, FROM_ADDRESS, 
            TO_ADDRESS, ETH_VALUE, TX_FEE, GAS_PRICE_BID, GAS_PRICE_PAID, GAS_LIMIT, GAS_USED, CUMULATIVE_GAS_USED, INPUT_DATA, STATUS
        from arbitrum.core.fact_transactions
        where block_number >= {{block_start}}
        and block_number < {{block_end}}
        order by block_number asc

    """


    # Optimism aggregations
    ,'optimism_txcount' :
    """
        SELECT 
            date_trunc('day', BLOCK_TIMESTAMP) AS day,
            count(*) AS val
        FROM optimism.core.fact_transactions
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x420000000000000000000000000000000000000f'
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'optimism_daa' :
    """
        SELECT 
            date_trunc('day', BLOCK_TIMESTAMP) AS day, 
            count(distinct "FROM_ADDRESS") AS val
        FROM optimism.core.fact_transactions
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x420000000000000000000000000000000000000f'
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'optimism_new_addresses' :
    """
        WITH first_tx AS (
        SELECT
            TO_ADDRESS AS address,
            min(BLOCK_TIMESTAMP) AS first_tx_ts 
        FROM optimism.core.fact_transactions
        GROUP BY 1)

        SELECT 
            date_trunc('day', first_tx_ts) AS day,
            count(distinct address) AS val
        FROM first_tx
        WHERE 
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
        GROUP BY 1
        ORDER BY day DESC
    """
    ,'optimism_fees_paid_usd' :
    """
        WITH eth_price AS (SELECT
            HOUR,
            PRICE
        FROM ethereum.core.fact_hourly_token_prices
        WHERE
            SYMBOL = 'WETH'
        ORDER BY 1 DESC)

        SELECT
            date_trunc('day', BLOCK_TIMESTAMP) as day,
            SUM(TX_FEE * PRICE) AS val
        FROM optimism.core.fact_transactions t
        LEFT JOIN eth_price e ON date_trunc('hour', t.BLOCK_TIMESTAMP) = e.hour
        WHERE
            day < CURRENT_DATE
            AND day >= dateadd(day, -{{Days}}, CURRENT_DATE)
            AND TO_ADDRESS <> '0x420000000000000000000000000000000000000f' 
        GROUP BY 1
        ORDER BY 1 DESC
    """

    # Optimism raw
    ,'optimism_transactions' : 
    """
        select 
            BLOCK_NUMBER, BLOCK_TIMESTAMP, BLOCK_HASH, TX_HASH, NONCE, POSITION, ORIGIN_FUNCTION_SIGNATURE, FROM_ADDRESS, TO_ADDRESS, ETH_VALUE, TX_FEE, GAS_PRICE, GAS_LIMIT, 
            GAS_USED, L1_GAS_PRICE, L1_GAS_USED, L1_FEE_SCALAR, L1_SUBMISSION_BATCH_INDEX, L1_SUBMISSION_TX_HASH, L1_STATE_ROOT_BATCH_INDEX, 
            L1_STATE_ROOT_TX_HASH, CUMULATIVE_GAS_USED, INPUT_DATA, STATUS
        from optimism.core.fact_transactions
        where block_number >= {{block_start}}
        and block_number < {{block_end}}
        order by block_number asc

    """

}
class FlipsideObject():
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

class FlipsideQuery(FlipsideObject):
    def __init__(self, sql: str, metric_key: str, origin_key: str, query_parameters: dict = None):
        self.sql_raw = sql 
        self.sql = self.replace_query_parameters(self.sql_raw, query_parameters)
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.last_token = None
        self.last_execution_loaded = None

class FlipsideRaw(FlipsideObject):
    def __init__(self, key:str, sql: str, table_name: str, s3_folder:str, query_parameters: dict = None, block_steps: int = 10000):
        self.key = key
        self.sql_raw = sql
        self.sql = self.replace_query_parameters(self.sql_raw, query_parameters)
        self.table_name = table_name
        self.query_parameters = query_parameters
        self.block_steps = block_steps
        self.s3_folder = s3_folder
        self.last_token = None
        self.last_execution_loaded = None
        



flipside_queries = [
    ## Layer 1s
    FlipsideQuery(metric_key = "txcount", origin_key = "ethereum", sql=flipside_sql["ethereum_txcount"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "daa", origin_key = "ethereum", sql=flipside_sql["ethereum_daa"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "waa", origin_key = "ethereum", sql=flipside_sql["ethereum_waa"], query_parameters={"Days": 1500})
    ,FlipsideQuery(metric_key = "maa", origin_key = "ethereum", sql=flipside_sql["ethereum_maa"], query_parameters={"Days": 1500})
    #,FlipsideQuery(metric_key = "new_addresses", origin_key = "ethereum", sql=flipside_sql["ethereum_new_addresses"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "fees_paid_usd", origin_key = "ethereum", sql=flipside_sql["ethereum_fees_paid_usd"], query_parameters={"Days": 7})

    ## Layer 2s
    ,FlipsideQuery(metric_key = "txcount", origin_key = "arbitrum", sql=flipside_sql["arbitrum_txcount"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "daa", origin_key = "arbitrum", sql=flipside_sql["arbitrum_daa"], query_parameters={"Days": 7})
    #,FlipsideQuery(metric_key = "new_addresses", origin_key = "arbitrum", sql=flipside_sql["arbitrum_new_addresses"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "fees_paid_usd", origin_key = "arbitrum", sql=flipside_sql["arbitrum_fees_paid_usd"], query_parameters={"Days": 7})

    ,FlipsideQuery(metric_key = "txcount", origin_key = "optimism", sql=flipside_sql["optimism_txcount"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "daa", origin_key = "optimism", sql=flipside_sql["optimism_daa"], query_parameters={"Days": 7})
    #,FlipsideQuery(metric_key = "new_addresses", origin_key = "optimism", sql=flipside_sql["optimism_new_addresses"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "fees_paid_usd", origin_key = "optimism", sql=flipside_sql["optimism_fees_paid_usd"], query_parameters={"Days": 7})


    ,FlipsideQuery(metric_key = "stables_mcap", origin_key = "imx", sql=flipside_sql["imx_stables"], query_parameters={"Days": 7})
    ,FlipsideQuery(metric_key = "stables_mcap", origin_key = "polygon_zkevm", sql=flipside_sql["polygonzkevm_stables"], query_parameters={"Days": 7})
    #,FlipsideQuery(metric_key = "stables_mcap", origin_key = "zksync_era", sql=flipside_sql["zksyncera_stables"], query_parameters={"Days": 7})
]



flipside_raws = [
    FlipsideRaw(key = 'arbitrum_tx', table_name = "arbitrum_tx", sql=flipside_sql["arbitrum_transactions"], query_parameters={"block_start": 100000000, "block_end": 100000001}, block_steps=20000, s3_folder="arbitrum")
    ,FlipsideRaw(key = 'optimism_tx', table_name = "optimism_tx", sql=flipside_sql["optimism_transactions"], query_parameters={"block_start": 100000000, "block_end": 100000001}, block_steps=80000, s3_folder="optimism")
]