import time
import pandas as pd
from web3 import Web3
import datetime
from web3.middleware import geth_poa_middleware

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for tracking stablecoin supply across different chains.
    
    This adapter tracks two types of stablecoins:
    1. Bridged stablecoins: Tokens that are locked in bridge contracts on the source chain
    2. Direct stablecoins: Tokens that are natively minted on the target chain
    
    adapter_params require the following fields:
        stables_metadata: dict - Metadata for stablecoins (name, symbol, decimals, etc.)
        stables_mapping: dict - Mapping of bridge contracts and token addresses
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Supply", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        self.stables_metadata = adapter_params['stables_metadata']
        self.stables_mapping = adapter_params['stables_mapping']
        
        # Initialize web3 connections to different chains
        self.connections = {}
        self.supported_chains = ['ethereum']  # Base chain
        
        # Add L2 chains that are in the mapping
        for chain_name in self.stables_mapping.keys():
            if chain_name not in self.supported_chains:
                self.supported_chains.append(chain_name)
        
        # Create connections to each chain
        for chain in self.supported_chains:
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

    def extract(self, load_params:dict):
        """
        Extract stablecoin data based on load parameters.
        
        load_params require the following fields:
            days: int - Days of historical data to load
            load_type: str - Type of data to load ('block_data', 'bridged_supply', 'direct_supply', 'total_supply')
            chains: list (optional) - Specific chains to process
            stablecoins: list (optional) - Specific stablecoins to track
        """
        self.days = load_params['days']
        self.load_type = load_params['load_type']
        self.chains = load_params.get('chains', self.supported_chains)
        self.stablecoins = load_params.get('stablecoins', list(self.stables_metadata.keys()))
        
        if self.load_type == 'block_data':
            df = self.get_block_data()
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
        Finds the first block of a given day using database caching and optimized search.
        
        Optimizations:
        1. Checks database for existing block data before making RPC calls
        2. Uses cache to avoid redundant lookups
        3. Makes smarter initial guesses based on block time estimates
        4. Implements early stopping for sequential date queries
        
        :param w3: Web3 object to connect to the blockchain.
        :param target_date: The target date to find the first block of the day (UTC).
        :return: Block object of the first block of the day or None if not found.
        """
        # Initialize cache if it doesn't exist yet
        if not hasattr(self, '_block_date_cache'):
            self._block_date_cache = {}
            self._avg_block_time = {}  # Cache for average block times per chain
            self._db_cache_loaded = False
        
        # Get chain ID to use for the cache key
        chain_id = w3.eth.chain_id
        
        # Try to identify which chain this is by name (for database lookups)
        chain_name = None
        for name, connection in self.connections.items():
            if connection == w3:
                chain_name = name
                break
        
        # Convert target date to timestamp
        start_of_day = datetime.datetime.combine(target_date, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
        start_timestamp = int(start_of_day.timestamp())
        
        # Check memory cache first
        if (chain_id, start_timestamp) in self._block_date_cache:
            print(f"Using memory-cached block for {target_date}")
            return self._block_date_cache[(chain_id, start_timestamp)]
        
        # Load from database if available and we haven't loaded it yet
        if chain_name and not self._db_cache_loaded:
            print(f"Loading cached blocks from database for {chain_name}")
            try:
                # Get all available first_block_of_day entries for this chain
                db_blocks = self.db_connector.get_data_from_table(
                    "fact_kpis",
                    filters={
                        "metric_key": "first_block_of_day",
                        "origin_key": chain_name
                    }
                )
                
                if not db_blocks.empty:
                    # Process database results to build cache
                    db_blocks = db_blocks.reset_index()
                    
                    for _, row in db_blocks.iterrows():
                        # We only have the block number from DB, not the full block
                        # We'll create a partial block object with just the number
                        # The timestamp will be filled when we need the actual block
                        date_obj = row['date']
                        day_start = datetime.datetime.combine(date_obj, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
                        day_timestamp = int(day_start.timestamp())
                        
                        # Create a placeholder block with just the number
                        # We'll fill in other details only if needed
                        self._block_date_cache[(chain_id, day_timestamp)] = {
                            'number': int(row['value']),
                            'db_placeholder': True  # Flag to indicate this is a placeholder
                        }
                    
                    # Also calculate average block time if we have multiple entries
                    if len(db_blocks) >= 2:
                        # Sort by date
                        db_blocks = db_blocks.sort_values(by='date')
                        
                        # Calculate the time difference and block difference
                        total_time_diff = 0
                        total_block_diff = 0
                        
                        for i in range(1, len(db_blocks)):
                            prev_date = db_blocks.iloc[i-1]['date']
                            curr_date = db_blocks.iloc[i]['date']
                            prev_block = int(db_blocks.iloc[i-1]['value']) 
                            curr_block = int(db_blocks.iloc[i]['value'])
                            
                            # Calculate differences
                            time_diff = (curr_date - prev_date).total_seconds()
                            block_diff = curr_block - prev_block
                            
                            if block_diff > 0:  # Avoid division by zero
                                total_time_diff += time_diff
                                total_block_diff += block_diff
                        
                        if total_block_diff > 0:
                            self._avg_block_time[chain_id] = total_time_diff / total_block_diff
                    
                    print(f"Loaded {len(db_blocks)} blocks from database for {chain_name}")
                    
                self._db_cache_loaded = True
                
            except Exception as e:
                print(f"Error loading blocks from database: {e}")
        
        # Check if we now have this block in cache after DB load
        if (chain_id, start_timestamp) in self._block_date_cache:
            block_info = self._block_date_cache[(chain_id, start_timestamp)]
            
            # If it's a placeholder from DB, fetch the full block
            if 'db_placeholder' in block_info and block_info['db_placeholder']:
                try:
                    full_block = w3.eth.get_block(block_info['number'])
                    self._block_date_cache[(chain_id, start_timestamp)] = full_block
                    return full_block
                except Exception as e:
                    print(f"Error fetching full block from placeholder: {e}")
                    # Fall through to regular search if this fails
            else:
                return block_info
        
        # Get latest block
        try:
            latest_block = w3.eth.get_block('latest')
            latest_block_number = latest_block['number']
            latest_timestamp = latest_block['timestamp']
        except Exception as e:
            print(f"Error getting latest block: {e}")
            return None
        
        # If our target date is in the future, return None early
        if start_timestamp > latest_timestamp:
            print(f"Target date {target_date} is in the future")
            return None
        
        # Check if we have any cached blocks for this chain that can help us narrow the search
        if hasattr(self, '_block_date_cache'):
            # Find the closest cached block before and after our target
            before_block = None
            after_block = None
            before_timestamp = 0
            after_timestamp = float('inf')
            
            for (c_id, ts), block in self._block_date_cache.items():
                if c_id != chain_id:
                    continue
                    
                # Skip placeholders for this part
                if 'db_placeholder' in block and block['db_placeholder']:
                    continue
                    
                if ts <= start_timestamp and ts > before_timestamp:
                    before_timestamp = ts
                    before_block = block
                
                if ts >= start_timestamp and ts < after_timestamp:
                    after_timestamp = ts
                    after_block = block
            
            # If we have blocks before and after, we can narrow our search range
            if before_block and after_block:
                low = before_block['number']
                high = after_block['number']
                #print(f"Narrowed search range using cache: {low} to {high}")
            
            # If we only have a block before, we can make a better guess
            elif before_block:
                # Estimate the average block time if we don't have it
                if chain_id not in self._avg_block_time:
                    # Estimate based on the last 1000 blocks (or fewer if we're near genesis)
                    blocks_to_check = min(1000, before_block['number'])
                    if blocks_to_check > 0:
                        old_block = w3.eth.get_block(before_block['number'] - blocks_to_check)
                        time_diff = before_block['timestamp'] - old_block['timestamp']
                        avg_time = time_diff / blocks_to_check
                        self._avg_block_time[chain_id] = avg_time
                    else:
                        # Default to 15 seconds for Ethereum if we can't calculate
                        self._avg_block_time[chain_id] = 15
                
                # Calculate estimated blocks between our target and the before block
                time_diff = start_timestamp - before_block['timestamp']
                est_blocks = int(time_diff / self._avg_block_time[chain_id])
                
                # Set search range with a safety margin
                low = before_block['number']
                high = min(before_block['number'] + est_blocks * 2, latest_block_number)
                #print(f"Estimated search range based on before block: {low} to {high}")
            else:
                # Default to full range if no useful cache entries
                low = 0
                high = latest_block_number
        else:
            # Default to full range if no cache
            low = 0
            high = latest_block_number
        
        # Now use placeholder info from DB to narrow search if available
        placeholder_blocks = []
        for (c_id, ts), block in self._block_date_cache.items():
            if c_id == chain_id and 'db_placeholder' in block and block['db_placeholder']:
                placeholder_blocks.append((ts, block['number']))
        
        if placeholder_blocks:
            # Sort by timestamp
            placeholder_blocks.sort()
            
            # Find blocks that bound our target timestamp
            before_ts = 0
            before_num = 0
            after_ts = float('inf')
            after_num = latest_block_number
            
            for ts, num in placeholder_blocks:
                if ts <= start_timestamp and ts > before_ts:
                    before_ts = ts
                    before_num = num
                if ts >= start_timestamp and ts < after_ts:
                    after_ts = ts
                    after_num = num
            
            # Narrow search range if we found bounding blocks
            if before_ts > 0 and after_ts < float('inf'):
                if before_num < low:
                    low = before_num
                if after_num < high:
                    high = after_num
                #print(f"Further narrowed search range using DB placeholders: {low} to {high}")
        
        # Binary search within our narrowed range
        while low < high:
            mid = (low + high) // 2
            try:
                mid_block = w3.eth.get_block(mid)
                time.sleep(0.1)  # Sleep to avoid rate limiting
                
                # Cache this block for future use
                day_start = datetime.datetime.fromtimestamp(mid_block['timestamp'], tz=datetime.timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                day_timestamp = int(day_start.timestamp())
                self._block_date_cache[(chain_id, day_timestamp)] = mid_block
                
                if mid_block['timestamp'] < start_timestamp:
                    low = mid + 1
                else:
                    high = mid
            except Exception as e:
                print(f"Error during binary search at block {mid}: {e}")
                # If we fail in the middle, try to recover by adjusting the search range
                # Assume the error was due to a non-existent block, so search lower
                high = mid - 1
        
        # Get the result block
        try:
            result_block = w3.eth.get_block(low)
            
            # Verify this block is actually on or after our target timestamp
            if result_block['timestamp'] < start_timestamp:
                # Try the next block if this one is too early
                try:
                    next_block = w3.eth.get_block(low + 1)
                    if next_block['timestamp'] >= start_timestamp:
                        result_block = next_block
                except:
                    # If there's no next block, just use what we have
                    pass
            
            # Cache the result
            self._block_date_cache[(chain_id, start_timestamp)] = result_block
            
            return result_block
        except Exception as e:
            print(f"Error getting result block {low}: {e}")
            return None
        
    def get_block_numbers(self, w3, days: int = 7):
        """
        Retrieves the first block of each day for the past 'days' number of days and returns a DataFrame 
        with the block number and timestamp for each day.

        :param w3: Web3 object to connect to the Ethereum blockchain.
        :param days: The number of days to look back from today (default is 7).
        :return: DataFrame containing the date, block number, and block timestamp for each day.
        """
        current_date = datetime.datetime.now().date()  # Get the current date (no time)
        start_date = current_date - datetime.timedelta(days=days)  # Calculate the start date

        # Initialize an empty list to hold the data
        block_data = []

        # Loop over each day from start_date to current_date
        while current_date > start_date:
            # Calculate the next day's date for which we want to find the first block
            target_date = start_date + datetime.timedelta(days=1)

            # Retrieve the first block of the day for the target date
            new_block = self.get_first_block_of_day(w3, target_date)

            # Error handling in case get_first_block_of_day returns None
            if new_block is None:
                print(f"ERROR: Could not retrieve block for {target_date}")
            else:
                # Log the block number and timestamp
                print(f'..block number for {target_date}: {new_block["number"]}')

                # Append the result as a dictionary to the block_data list
                block_data.append({
                    'date': str(target_date),
                    'block': new_block['number'],
                    'block_timestamp': new_block['timestamp']
                })

            # Move to the next date
            start_date = target_date

        # Convert the collected block data into a DataFrame
        df = pd.DataFrame(block_data)

        # block as string
        df['block'] = df['block'].astype(str)
        return df

    def get_block_data(self):
        """
        Get block data for all chains
        
        First checks if data already exists in the database (fact_kpis table) to avoid
        unnecessary RPC calls. Only fetches missing data from the blockchain.
        """
        df_main = pd.DataFrame()
        
        for chain in self.chains:
            print(f"Processing {chain} block data")
            
            # First check if we already have this data in the database
            existing_data = self.db_connector.get_data_from_table(
                "fact_kpis", 
                filters={
                    "metric_key": "first_block_of_day",
                    "origin_key": chain
                },
                days=self.days
            )
            
            if not existing_data.empty and len(existing_data) >= self.days:
                print(f"...using existing block data for {chain} from database")
                # Rename 'value' column to match expected format
                existing_data = existing_data.reset_index()
                df_chain = existing_data[['metric_key', 'origin_key', 'date', 'value']]
                df_main = pd.concat([df_main, df_chain])
                continue
            
            # If we don't have complete data, check what dates we're missing
            existing_dates = set()
            if not existing_data.empty:
                print(f"...found partial data for {chain}, fetching missing dates only")
                existing_data = existing_data.reset_index()
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
            
            # Find missing dates
            missing_dates = all_dates - existing_dates
            
            if not missing_dates:
                print(f"...no missing dates for {chain}")
                continue
                
            print(f"...fetching {len(missing_dates)} missing dates for {chain}")
            
            # Get web3 connection for this chain
            w3 = self.connections[chain]
            
            # Only fetch missing dates
            missing_blocks_data = []
            for date_str in sorted(missing_dates):
                target_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                
                # Skip dates in the future
                if target_date > current_date:
                    continue
                    
                # Get first block of this day
                block = self.get_first_block_of_day(w3, target_date)
                
                if block:
                    missing_blocks_data.append({
                        'date': date_str,
                        'block': block['number'],
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
                    
                df_main = pd.concat([df_main, df_chain])
        
        # Remove duplicates and set index
        if not df_main.empty:
            df_main.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
            df_main.set_index(['metric_key', 'origin_key', 'date'], inplace=True)

            ## if col index in df_main, drop it
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
        
        df_blocknumbers['block'] = df_blocknumbers['value'].astype(int).astype(str)
        df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
        df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=False)

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
                            
                        df.loc[i, 'value'] = total_balance
                    
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
            
            df_blocknumbers['block'] = df_blocknumbers['value'].astype(int).astype(str)
            df_blocknumbers.drop(columns=['value', 'origin_key'], inplace=True)
            df_blocknumbers = df_blocknumbers.sort_values(by='block', ascending=False)
            
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
                        df.loc[i, 'value'] = adjusted_supply
                        
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