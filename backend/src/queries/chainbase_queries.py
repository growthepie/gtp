## Dict that stores all SQL code for different queries. Parameters are passed in as {{parameter_name}} and are replaced in the code below.
chainbase_sql = {
    ##################### Layer 1s #####################
  
    ## Arbitrum raw
    'arbitrum_transactions' : 
    """
        select 
            block_number, 
            block_timestamp, 
            block_hash, 
            transaction_hash as tx_hash, 
            nonce, 
            transaction_index as "position", 
            from_address, 
            to_address, 
            value as eth_value,
            (gas_used / POWER(10,18)) * effective_gas_price as tx_fee,
            gas_price as gas_price_bid, 
            effective_gas_price as gas_price_paid, 
            gas as gas_limit, 
            gas_used, 
            cumulative_gas_used, 
            input as input_data, 
            status
        from arbitrum.transactions
        where block_number >= {{block_start}}
        and block_number < {{block_end}}
        order by block_number asc
    """

    # Optimism raw
    ,'optimism_transactions' : 
    """
        select 
            block_number, 
            block_timestamp, 
            block_hash, 
            transaction_hash as tx_hash, 
            nonce, 
            transaction_index as "position", 
            from_address, 
            to_address, 
            value as eth_value, 
            -1 as tx_fee,
            gas_price, 
            gas as gas_limit,
            gas_used,
            cumulative_gas_used,
            input as input_data,
            status
        from optimism.transactions
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