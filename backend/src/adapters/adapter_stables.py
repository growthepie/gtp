import time
import pandas as pd
from web3 import Web3
import datetime
from web3.middleware import geth_poa_middleware

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

## TODO: add days 'auto' functionality. if blocks are missing, fetch all. If tokens are missing, fetch all
## This should also work for new tokens being added etc
## TODO: add functionality that for some chains we don't need block data (we only need it if we have direct tokens)

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for tracking stablecoin supply across different chains.
    
    This adapter tracks two types of stablecoins:
    1. Bridged stablecoins: Tokens that are locked in bridge contracts on the source chain
    2. Direct stablecoins: Tokens that are natively minted on the target chain
    
    adapter_params require the following fields:
        stables_metadata: dict - Metadata for stablecoins (name, symbol, decimals, etc.)
        stables_mapping: dict - Mapping of bridge contracts and token addresses
        origin_keys: list (optional) - Specific chains to process
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Supply", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        self.stables_metadata = adapter_params['stables_metadata']
        self.stables_mapping = adapter_params['stables_mapping']
        
        # Initialize web3 connections to different chains
        self.connections = {}
        self.supported_chains = ['ethereum']  # Default supported chains
        
        # Add L2 chains that are in the mapping
        for chain_name in self.stables_mapping.keys():
            if chain_name not in self.supported_chains:
                self.supported_chains.append(chain_name)

        self.chains = adapter_params.get('origin_keys', self.supported_chains)
        self.chains.append('ethereum')  # Always include Ethereum as source chain
        
        # Create connections to each chain
        for chain in self.chains:
            try:
                rpc_url = self.db_connector.get_special_use_rpc(chain)
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                
                # Apply middleware for PoA chains if needed
                if chain != 'ethereum':
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                    
                self.connections[chain] = w3
                print(f"Connected to {chain}")
            except Exception as e:
                print(f"Failed to connect to {chain}: {e}")
        
        print_init(self.name, self.adapter_params)

    def extract(self, load_params:dict, update=False):
        """
        Extract stablecoin data based on load parameters.
        
        load_params require the following fields:
            days: int - Days of historical data to load
            load_type: str - Type of data to load ('block_data', 'bridged_supply', 'direct_supply', 'total_supply')
            stablecoins: list (optional) - Specific stablecoins to track
        """
        self.days = load_params['days']
        self.load_type = load_params['load_type']
        self.stablecoins = load_params.get('stablecoins', list(self.stables_metadata.keys()))
        
        if self.load_type == 'block_data':
            df = self.get_block_data(update=update)
        elif self.load_type == 'bridged_supply':
            df = self.get_bridged_supply()
        elif self.load_type == 'direct_supply':
            df = self.get_direct_supply()
        elif self.load_type == 'total_supply':
            df = self.get_total_supply()
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")

        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df:pd.DataFrame):
        """Load processed data into the database"""
        if self.load_type == 'block_data' or self.load_type == 'total_supply':
            tbl_name = 'fact_kpis'
        else:
            tbl_name = 'fact_stables'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    #### Helper functions ###
    def get_block_date(self, w3: Web3, block_number: int):
        """
        Get the date of a block based on its block number.

        :param w3: Web3 object to connect to the EVM blockchain.
        :param block_number: Block number to find the date for.
        :return: Date of the block or None if the block is not found.
        """
        day_unix = w3.eth.get_block(block_number)['timestamp']
        day = datetime.datetime.utcfromtimestamp(day_unix)
        return day

    def get_erc20_balance(self, w3: Web3, token_contract: str, token_abi: dict, address, at_block='latest'):
        """
        Retrieves the ERC20 token balance for a given token contract and address at a specified block.

        :param w3: Web3 object to connect to EVM blockchain.
        :param token_contract: Address of the ERC20 token contract.
        :param token_abi: ABI of the ERC20 token contract.
        :param address: EVM address to check the balance.
        :param at_block: Block number to get the balance (default is 'latest').
        :return: Token balance for the specified address.
        """
        if not Web3.is_address(address):
            print(f"Invalid address: {address}")
            raise ValueError(f"Invalid address: {address}")

        result = self.call_contract_function(w3, token_contract, token_abi, 'balanceOf', Web3.to_checksum_address(address), at_block=at_block)
        if result is None:  # Check for None to avoid division by zero
            print(f"Error retrieving ERC20 balance for {address} with block {at_block}")
            return None
        
        return result / 10**18
    
    def call_contract_function(self, w3: Web3, contract_address: str, abi: dict, function_name: str, *args, at_block='latest'):
        """
        Calls a specific function of a contract and handles errors gracefully.

        :param w3: Web3 object to connect to the EVM blockchain.
        :param contract_address: Address of the contract.
        :param abi: ABI of the contract.
        :param function_name: Name of the function to call on the contract.
        :param args: Arguments to pass to the contract function.
        :param at_block: Block identifier to execute the call.
        :return: The result of the contract function or None if an error occurs.
        """

        # check if the contract was deployed at the given address
        code = w3.eth.get_code(contract_address, block_identifier=at_block)
        time.sleep(0.1)  # Sleep to avoid rate limiting
        if code == b'':  # Contract not deployed by this block
            print(f"Contract not deployed at address {contract_address} with block {at_block}")
            return None

        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)
            time.sleep(0.1)  # Sleep to avoid rate limiting
            function = getattr(contract.functions, function_name)
            return function(*args).call(block_identifier=int(at_block))
        except Exception as e:
            print(f"Error calling function {function_name} with args {args} on contract {contract_address} with block_identifier {at_block}: {e}")
            ## print datatypes of all variables
            print(f"contract_address: {type(contract_address)}")
            print(f"abi: {type(abi)}")
            print(f"function_name: {type(function_name)}")
            print(f"at_block: {type(at_block)}")
            ## print all args types
            for i in range(len(args)):
                print(f"args[{i}]: {type(args[i])}")

            raise e
        
    def get_first_block_of_day(self, w3: Web3, target_date: datetime.date):
        """
        Finds the first block of a given day using binary search based on the timestamp.
        Includes simple optimizations to reduce RPC calls.

        :param w3: Web3 object to connect to Ethereum blockchain.
        :param target_date: The target date to find the first block of the day (in UTC).
        :return: Block object of the first block of the day or None if not found.
        """
        # Initialize cache if not exists
        if not hasattr(self, '_block_cache'):
            self._block_cache = {}  # Simple cache: {(chain_id, date_str): block}
            self._timestamp_cache = {}  # Cache: {(chain_id, block_num): timestamp}
        
        # Get chain ID for cache lookup
        chain_id = w3.eth.chain_id
        date_str = target_date.strftime("%Y-%m-%d")
        
        # Check cache first
        cache_key = (chain_id, date_str)
        if cache_key in self._block_cache:
            print(f"Using cached block for {date_str}")
            return self._block_cache[cache_key]
        
        # Calculate start timestamp for target day
        start_of_day = datetime.datetime.combine(target_date, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
        start_timestamp = int(start_of_day.timestamp())

        # Get latest block
        latest_block = w3.eth.get_block('latest')
        latest_number = latest_block['number']
        
        # Cache the latest block timestamp
        self._timestamp_cache[(chain_id, latest_number)] = latest_block['timestamp']
        
        # Early exit if chain didn't exist yet
        if latest_block['timestamp'] < start_timestamp:
            return None

        low, high = 0, latest_number

        # Binary search to find the first block with timestamp >= start_timestamp
        while low < high:
            mid = (low + high) // 2
            
            # Check cache before making RPC call
            if (chain_id, mid) in self._timestamp_cache:
                mid_timestamp = self._timestamp_cache[(chain_id, mid)]
            else:
                try:
                    mid_block = w3.eth.get_block(mid)
                    mid_timestamp = mid_block['timestamp']
                    # Cache this result
                    self._timestamp_cache[(chain_id, mid)] = mid_timestamp
                    time.sleep(0.1)  # Sleep to avoid rate limiting
                except Exception as e:
                    print(f"Error getting block {mid}: {e}")
                    # On error, adjust bounds to try a different block
                    high = mid - 1
                    continue
            
            if mid_timestamp < start_timestamp:
                low = mid + 1
            else:
                high = mid

        # Get the final block
        try:
            result_block = w3.eth.get_block(low)
            # Cache for future use
            self._block_cache[cache_key] = result_block
            return result_block if result_block['timestamp'] >= start_timestamp else None
        except Exception as e:
            print(f"Error getting final block {low}: {e}")
            return None
        
    def get_block_numbers(self, w3, days: int = 7):
        """
        Retrieves the first block of each day for the past 'days' number of days and returns a DataFrame 
        with the block number and timestamp for each day.
        
        Processes dates from newest to oldest for consistency with get_block_data method.

        :param w3: Web3 object to connect to the Ethereum blockchain.
        :param days: The number of days to look back from today (default is 7).
        :return: DataFrame containing the date, block number, and block timestamp for each day.
        """
        current_date = datetime.datetime.now().date()  # Get the current date (no time)
        
        # Initialize an empty list to hold the data
        block_data = []
        found_zero_block = False

        # Process dates from newest to oldest
        for i in range(days):
            target_date = current_date - datetime.timedelta(days=i)
            
            # If we found a zero block already, skip older dates
            if found_zero_block:
                print(f"..skipping {target_date} as earlier date had zero block")
                continue
                
            # Retrieve the first block of the day for the target date
            block = self.get_first_block_of_day(w3, target_date)

            # Error handling in case get_first_block_of_day returns None
            if block is None:
                print(f"ERROR: Could not retrieve block for {target_date}")
            else:
                # Log the block number and timestamp
                print(f'..block number for {target_date}: {block["number"]}')
                
                # Check if this is a zero block (chain didn't exist yet)
                if block["number"] == 0:
                    found_zero_block = True
                    print(f"..found zero block at {target_date}, will skip older dates")

                # Append the result as a dictionary to the block_data list
                block_data.append({
                    'date': str(target_date),
                    'block': block['number'],
                    'block_timestamp': block['timestamp']
                })

        # Convert the collected block data into a DataFrame
        df = pd.DataFrame(block_data)
        
        # Handle empty dataframe case
        if df.empty:
            return df
            
        # Convert block to string
        df['block'] = df['block'].astype(str)
        return df

    def get_block_data(self, update=False):
        """
        Get block data for all chains
        
        First checks if data already exists in the database (fact_kpis table) to avoid
        unnecessary RPC calls. Only fetches missing data from the blockchain.
        
        Optimizations:
        1. Uses existing data from database when available
        2. Only fetches missing dates
        3. Stops processing older dates once block 0 is found (chain wasn't active)
        4. Filters out all but the latest date with block 0
        """
        df_main = pd.DataFrame()
        print(f"Getting block data for {self.days} days and update set to {update}")
        
        for chain in self.chains:
            print(f"Processing {chain} block data")
            ## check if chain dict has a key "direct" and if it contains data
            if chain != 'ethereum' and (self.stables_mapping[chain].get("direct") is None or len(self.stables_mapping[chain]["direct"]) == 0):
                print(f"Skipping {chain} as it doesn't have direct tokens")
                continue
            
            # First check if we already have this data in the database
            existing_data = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": chain
                },
                days=self.days
            )
            
            # Check if we already have complete data in the database
            if not existing_data.empty and len(existing_data) >= self.days:
                print(f"...using existing block data for {chain} from database")
                # Rename 'value' column to match expected format
                existing_data = existing_data.reset_index()
                
                # Handle zero blocks - keep only the latest date with block 0
                if 0 in existing_data['value'].astype(int).values:
                    existing_data_with_zeroes = existing_data[existing_data['value'].astype(int) == 0]
                    latest_zero_date = existing_data_with_zeroes['date'].max()
                    
                    # Keep all non-zero blocks and only the latest zero block
                    existing_data = existing_data[
                        (existing_data['value'].astype(int) != 0) | 
                        (existing_data['date'] == latest_zero_date)
                    ]
                    
                    print(f"...filtered out old zero blocks, keeping only latest at {latest_zero_date}")
                
                df_chain = existing_data[['metric_key', 'origin_key', 'date', 'value']]
                df_main = pd.concat([df_main, df_chain])
                continue
            
            # If we don't have complete data, check what dates we're missing
            existing_dates = set()
            known_zero_date = None
            
            if not existing_data.empty:
                print(f"...found partial data for {chain}, fetching missing dates only")
                existing_data = existing_data.reset_index()
                
                # Check if we have any dates with block 0 already
                zero_blocks = existing_data[existing_data['value'].astype(int) == 0]
                if not zero_blocks.empty:
                    known_zero_date = zero_blocks['date'].max()
                    print(f"...found existing zero block at {known_zero_date}, will skip older dates")
                
                existing_dates = set(existing_data['date'].dt.strftime('%Y-%m-%d'))
            
            # Check if chain connection is available
            if chain not in self.connections:
                print(f"...skipping {chain} - no connection available")
                continue
                
            # Create set of all dates we need
            current_date = datetime.datetime.now().date()
            all_dates = set()
            for i in range(self.days):
                date = current_date - datetime.timedelta(days=i)
                all_dates.add(date.strftime('%Y-%m-%d'))
            
            # Find missing dates, sorted from newest to oldest
            missing_dates = sorted(all_dates - existing_dates, reverse=True)
            
            if not missing_dates:
                print(f"...no missing dates for {chain}")
                continue
                
            print(f"...fetching {len(missing_dates)} missing dates for {chain}")
            
            # Get web3 connection for this chain
            w3 = self.connections[chain]
            
            # Only fetch missing dates
            missing_blocks_data = []
            found_zero_block = False
            oldest_zero_date = None
            
            for date_str in missing_dates:  # Already sorted newest to oldest
                target_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                
                # Skip dates in the future
                if target_date > current_date:
                    continue
                    
                # Skip dates older than our known zero block date (if we have one)
                if known_zero_date and target_date < known_zero_date.date():
                    #print(f"...skipping {date_str} as it's older than known zero block date {known_zero_date.date()}")
                    continue
                    
                # If we already found a zero block in this run, skip older dates
                if found_zero_block and target_date < oldest_zero_date:
                    #print(f"...skipping {date_str} as it's older than discovered zero block date {oldest_zero_date}")
                    continue
                    
                # Get first block of this day
                block = self.get_first_block_of_day(w3, target_date)
                
                if block:
                    block_number = block['number']
                    
                    # Check if this is a zero block
                    if block_number == 0:
                        found_zero_block = True
                        oldest_zero_date = target_date
                        print(f"...found zero block at {date_str}, will skip older dates")
                    
                    missing_blocks_data.append({
                        'date': date_str,
                        'block': block_number,
                        'block_timestamp': block['timestamp'],
                        'origin_key': chain,
                        'metric_key': 'first_block_of_day'
                    })
                else:
                    print(f"...couldn't get block for {date_str} on {chain}")
            
            # Create dataframe from missing blocks
            if missing_blocks_data:
                df_missing = pd.DataFrame(missing_blocks_data)
                
                # Convert block to string and date to datetime
                df_missing['block'] = df_missing['block'].astype(str)
                df_missing['date'] = pd.to_datetime(df_missing['date'])
                
                # Rename block column to value and drop block_timestamp
                df_missing.rename(columns={'block': 'value'}, inplace=True)
                if 'block_timestamp' in df_missing.columns:
                    df_missing.drop(columns=['block_timestamp'], inplace=True)
                
                # Combine with existing data if present
                if not existing_data.empty:
                    df_chain = pd.concat([existing_data, df_missing])
                else:
                    df_chain = df_missing
                
                # Handle zero blocks - keep only the latest date with block 0
                if 0 in df_chain['value'].astype(int).values:
                    df_chain_with_zeroes = df_chain[df_chain['value'].astype(int) == 0]
                    latest_zero_date = df_chain_with_zeroes['date'].max()
                    
                    # Keep all non-zero blocks and only the latest zero block
                    df_chain = df_chain[
                        (df_chain['value'].astype(int) != 0) | 
                        (df_chain['date'] == latest_zero_date)
                    ]
                    
                    print(f"...filtered out old zero blocks, keeping only latest at {latest_zero_date}")
                
                if update:
                    df_chain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
                    df_chain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)

                    # If col index in df_main, drop it
                    if 'index' in df_chain.columns:
                        df_chain.drop(columns=['index'], inplace=True)
                    self.load(df_chain)

                df_main = pd.concat([df_main, df_chain])
        
        # Remove duplicates and set index
        if not df_main.empty:
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
            df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)

            # If col index in df_main, drop it
            if 'index' in df_main.columns:
                df_main.drop(columns=['index'], inplace=True)
        
        return df_main
    
    def get_bridged_supply(self):
        """
        Get supply of stablecoins locked in bridge contracts on Ethereum
        """
        # Get block numbers for Ethereum
        df_blocknumbers = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": 'ethereum'
                },
                days=self.days
            )
        
        if df_blocknumbers.empty:
            print("No block data for Ethereum, generating...")
            raise ValueError("No block data for Ethereum")
        
        df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
        df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
        df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=True)
        df_blocknumbers['block'] = df_blocknumbers['block'].astype(str)

        df_main = pd.DataFrame()
        
        # Process each L2 chain
        for chain in self.chains:
            if chain == 'ethereum':
                continue  # Skip Ethereum as it's the source chain
            
            if chain not in self.stables_mapping:
                print(f"No mapping found for {chain}, skipping")
                continue
            
            print(f"Processing bridged stablecoins for {chain}")
            
            # Check if chain has bridged tokens
            if 'bridged' not in self.stables_mapping[chain]:
                print(f"No bridged tokens defined for {chain}")
                continue
            
            # Get bridge contracts for this chain
            bridge_config = self.stables_mapping[chain]['bridged']

            # Get date of first block of this chain
            if chain not in self.connections:
                raise ValueError(f"Chain {chain} not connected to RPC, please add RPC connection (assign special_use in sys_rpc_config)")
            first_block_date = self.get_block_date(self.connections[chain], 1)
            print(f"First block date for {chain}: {first_block_date}")

            # Process each source chain (usually Ethereum)
            ## TODO: add support for other source chains
            for source_chain, bridge_addresses in bridge_config.items():
                if source_chain != 'ethereum':
                    print(f"Skipping source chain {source_chain} for {chain} - only Ethereum supported so far")
                    continue

                if source_chain not in self.connections:
                    print(f"Source chain {source_chain} not connected, skipping")
                    continue
                
                # Get web3 connection for source chain
                w3 = self.connections[source_chain]
                
                # Check balances for all stablecoins in all bridge addresses
                for stablecoin_id in self.stablecoins:
                    if stablecoin_id not in self.stables_metadata:
                        print(f"Stablecoin {stablecoin_id} not in metadata, skipping")
                        continue
                    
                    stable_data = self.stables_metadata[stablecoin_id]
                    symbol = stable_data['symbol']
                    decimals = stable_data['decimals']
                    
                    print(f"Checking {symbol} locked in {chain} bridges")
                    
                    # Surce chain token address (e.g., USDT on Ethereum)
                    token_address = self.stables_metadata[stablecoin_id]['addresses'][source_chain]
                    
                    # Basic ERC20 ABI for balanceOf function
                    token_abi = [
                        {"constant":True,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}
                    ]
                    
                    # Create a DataFrame for this stablecoin
                    df = df_blocknumbers.copy()
                    df['origin_key'] = chain
                    df['token_key'] = stablecoin_id
                    df['value'] = 0.0  # Initialize balance column
                    
                    # Create contract instance
                    try:
                        token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
                    except Exception as e:
                        print(f"Failed to create contract instance for {stablecoin_id}: {e}")
                        continue
                    
                    # Check balance in each bridge address for each block
                    contract_deployed = True
                    for i in range(len(df)-1, -1, -1):  # Go backwards in time
                        date = df['date'].iloc[i]
                        if date < first_block_date:
                            print(f"Reached first block date ({first_block_date}) for {chain}, stopping")
                            break  # Stop if we reach the first block date

                        block = df['block'].iloc[i]
                        print(f"...retrieving bridged balance for {symbol} at block {block} ({date})")
                        
                        total_balance = 0
                        
                        # Sum balances across all bridge addresses
                        for bridge_address in bridge_addresses:
                            try:
                                # Call balanceOf function
                                balance = token_contract.functions.balanceOf(
                                    Web3.to_checksum_address(bridge_address)
                                ).call(block_identifier=int(block))
                                
                                # Convert to proper decimal representation
                                adjusted_balance = balance / (10 ** decimals)
                                total_balance += adjusted_balance
                                
                            except Exception as e:
                                print(f"....Error getting balance for {symbol} in {bridge_address} at block {block}: {e}")
                                if 'execution reverted' in str(e):
                                    # Contract might not be deployed yet
                                    contract_deployed = False
                                    break
                        
                        if not contract_deployed:
                            print(f"Contract for {symbol} not deployed at block {block}, stopping")
                            break
                        
                        df.loc[df.index[i], 'value'] = total_balance
                    
                    df_main = pd.concat([df_main, df])
        
        # Create metric_key column 
        df_main['metric_key'] = 'supply_bridged'
        
        # Drop unneeded columns
        df_main.drop(columns=['block'], inplace=True)

        # Clean up data
        df_main = df_main[df_main['value'] != 0]
        df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        df_main = df_main.dropna()
        df_main.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        return df_main
    
    def get_direct_supply(self):
        """
        Get supply of stablecoins that are natively minted on L2 chains
        """
        df_main = pd.DataFrame()
        
        # Process each chain
        for chain in self.chains:
            if chain not in self.stables_mapping:
                print(f"No mapping found for {chain}, skipping")
                continue
            
            if chain not in self.connections:
                print(f"Chain {chain} not connected, skipping")
                continue
                
            # Check if chain has direct tokens
            if 'direct' not in self.stables_mapping[chain]:
                print(f"No direct tokens defined for {chain}")
                continue
                
            print(f"Processing direct stablecoins for {chain}")
            
            # Get block numbers for this chain
            df_blocknumbers = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": chain
                },
                days=self.days
            )

            df_blocknumbers['block'] = df_blocknumbers['value'].astype(int)
            df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
            df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=True)
            df_blocknumbers['block'] = df_blocknumbers['block'].astype(str)
            
            # Get web3 connection for this chain
            w3 = self.connections[chain]
            
            # Get direct token configs for this chain
            direct_config = self.stables_mapping[chain]['direct']
            
            # Process each stablecoin
            for stablecoin_id, token_config in direct_config.items():
                if stablecoin_id not in self.stables_metadata:
                    print(f"Stablecoin {stablecoin_id} not in metadata, skipping")
                    continue
                
                if stablecoin_id not in self.stablecoins:
                    print(f"Stablecoin {stablecoin_id} not in requested stablecoins, skipping")
                    continue
                
                stable_data = self.stables_metadata[stablecoin_id]
                symbol = stable_data['symbol']
                decimals = stable_data['decimals']
                
                print(f"Getting supply for {symbol} on {chain}")
                
                # Extract token details
                token_address = token_config['token_address']
                method_name = token_config['method_name']
                
                # Basic ABI for totalSupply and decimals
                token_abi = [
                    {"constant":True,"inputs":[],"name":method_name,"outputs":[{"name":"","type":"uint256"}],"type":"function"},
                    {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}
                ]
                
                # Create a DataFrame for this stablecoin
                df = df_blocknumbers.copy()
                df['origin_key'] = chain
                df['token_key'] = stablecoin_id
                df['value'] = 0.0
                
                # Create contract instance
                try:
                    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
                except Exception as e:
                    print(f"Failed to create contract instance for {stablecoin_id} on {chain}: {e}")
                    continue
                
                # Query total supply for each block
                contract_deployed = True
                for i in range(len(df)-1, -1, -1):  # Go backwards in time
                    date = df['date'].iloc[i]
                    block = df['block'].iloc[i]
                    print(f"...retrieving direct supply for {symbol} at block {block} ({date})")
                    
                    try:
                        # Call totalSupply function (or custom method name)
                        supply_func = getattr(token_contract.functions, method_name)
                        total_supply = supply_func().call(block_identifier=int(block))
                        
                        # Convert to proper decimal representation
                        adjusted_supply = total_supply / (10 ** decimals)
                        df.loc[df.index[i], 'value'] = adjusted_supply
                        
                    except Exception as e:
                        print(f"....Error getting total supply for {symbol} at block {block}: {e}")
                        if 'execution reverted' in str(e):
                            # Contract might not be deployed yet
                            contract_deployed = False
                            break
                
                if not contract_deployed:
                    print(f"Contract for {symbol} not deployed at that time, skipping older blocks")
                    # Still add what we have so far
                
                df_main = pd.concat([df_main, df])
        
        # Create metric_key column 
        df_main['metric_key'] = 'supply_direct'
        
        # Drop unneeded columns
        df_main.drop(columns=['block'], inplace=True)

        # Clean up data
        df_main = df_main[df_main['value'] != 0]
        df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        df_main = df_main.dropna()
        df_main.set_index(['metric_key', 'origin_key', 'date', 'token_key'], inplace=True)
        return df_main
    
    def get_total_supply(self):
        """
        Calculate the total stablecoin supply (bridged + direct) per chain
        
        This method:
        1. Retrieves bridged supply data from Ethereum bridge contracts
        2. Retrieves direct supply data from L2 native tokens
        3. Combines them to get total stablecoin supply by chain and stablecoin
        4. Also calculates a total across all stablecoins for each chain
        """
        # Check if we have existing data in the database
        df_bridged = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "supply_bridged"
                    },
                    days=self.days
                )
        df_direct = self.db_connector.get_data_from_table("fact_stables",
                    filters={
                        "metric_key": "supply_direct"
                    },
                    days=self.days
                )
        
        # Reset index to work with the dataframes
        if not df_bridged.empty:
            df_bridged = df_bridged.reset_index()
        if not df_direct.empty:
            df_direct = df_direct.reset_index()
        
        # Combine both datasets
        df = pd.concat([df_bridged, df_direct])
        
        if df.empty:
            print("No data available for total supply calculation")
            # Return empty dataframe with correct structure
            return pd.DataFrame(columns=['metric_key', 'origin_key', 'date', 'token_key', 'value']).set_index(['metric_key', 'origin_key', 'date', 'token_key'])
        
        # Also create total across all stablecoins
        df_total = df.groupby(['origin_key', 'date'])['value'].sum().reset_index()
        df_total['metric_key'] = 'stables_mcap'
        
        # Set index and return
        df_total.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return df_total