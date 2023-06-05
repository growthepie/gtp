## Dict that stores all SQL code for different queries. Parameters are passed in as {{parameter_name}} and are replaced in the code below.
chainbase_sql = {
    ##################### Layer 1s #####################
  
    ## Arbitrum raw
    'arbitrum_transactions' : 
    """
        select 
            BLOCK_NUMBER, BLOCK_TIMESTAMP, BLOCK_HASH, TX_HASH, NONCE, POSITION, ORIGIN_FUNCTION_SIGNATURE, FROM_ADDRESS, 
            TO_ADDRESS, ETH_VALUE, TX_FEE, GAS_PRICE_BID, GAS_PRICE_PAID, GAS_LIMIT, GAS_USED, CUMULATIVE_GAS_USED, INPUT_DATA, STATUS
        from arbitrum.core.fact_transactions
        where block_number >= {{block_start}}
        and block_number < {{block_end}}
        order by block_number asc

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

class ChainbaseObject():
    ## replace_query_parameters
    def replace_query_parameters(self, sql: str, query_parameters: dict) -> str:
        for key, value in query_parameters.items():
            sql = sql.replace("{{" + key + "}}", str(value))
        return sql  

    def update_query_parameters(self, query_parameters: dict):
        for key in query_parameters.keys():
            self.query_parameters[key] = query_parameters[key]
        self.sql = self.replace_query_parameters(self.sql_raw, self.query_parameters)

class ChainbaseRaw(ChainbaseObject):
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


chainbase_raws = [
    ChainbaseRaw(key = 'arbitrum_tx', table_name = "arbitrum_tx", sql=chainbase_sql["arbitrum_transactions"], query_parameters={"block_start": 100000000, "block_end": 100000001}, block_steps=5000, s3_folder="arbitrum")
    ,ChainbaseRaw(key = 'optimism_tx', table_name = "optimism_tx", sql=chainbase_sql["optimism_transactions"], query_parameters={"block_start": 100000000, "block_end": 100000001}, block_steps=10000, s3_folder="optimism")
]