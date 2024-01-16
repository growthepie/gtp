from src.adapters.abstract_adapters import AbstractAdapterRaw
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from src.adapters.adapter_utils import *
import requests

class LoopringAdapter(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Loopring", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.url = adapter_params['api_url']
        self.table_name = f'{self.chain}_tx'   
        self.db_connector = db_connector
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
        
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.threads = load_params['threads']
        self.run(self.block_start, self.batch_size, self.threads)
        print(f"FINISHED loading raw tx data for {self.chain}.")
        
    def get_account_address(self, account_id):
        url = f"{self.url}/account?accountId={account_id}"
        response = requests.get(url)
        if response.status_code == 200:
            account_data = response.json()
            return account_data.get('owner')
        else:
            return None
        
    def get_block_data(self,block_id):
        url = f"{self.url}/block/getBlock?id={block_id}"
        response = requests.get(url)

        if response.status_code == 200:
            block_data = response.json()
            block_number = block_data.get('blockId')
            block_timestamp = block_data.get('createdAt')
            transactions_list = []

            for index, transaction in enumerate(block_data.get('transactions', [])):
                tx_id = f"{block_id}-{index}"
                tx_type = transaction.get('txType')
                tx_info = {
                    'tx_id': tx_id,
                    'tx_type': tx_type,
                    'fee_token_id': transaction.get('fee', {}).get('tokenId'),
                    'fee_amount': transaction.get('fee', {}).get('amount'),
                    'from_account_id': transaction.get('accountId', 'Unavailable'),
                    'to_account_id': transaction.get('toAccountId', 'Unavailable'),
                    'from_address': 'Unavailable',
                    'to_address': 'Unavailable',
                    'block_timestamp': block_timestamp,
                    'block_number': block_number,
                }

                # Extracting from_address and to_address
                if 'fromAddress' in transaction:
                    tx_info['from_address'] = transaction['fromAddress']
                elif 'orderA' in transaction and 'orderB' in transaction:
                    from_account_id = transaction['orderA'].get('accountID')
                    if from_account_id:
                        tx_info['from_account_id'] = from_account_id
                        tx_info['from_address'] = self.get_account_address(from_account_id)
                elif 'owner' in transaction:
                    tx_info['from_address'] = transaction.get('owner')
                elif 'accountId' in transaction:
                    tx_info['from_address'] = self.get_account_address(transaction.get('accountId'))

                to_address = transaction.get('toAddress') or transaction.get('toAccountAddress')
                if to_address:
                    tx_info['to_address'] = to_address
                elif 'toAccountId' in transaction:
                    tx_info['to_address'] = self.get_account_address(transaction.get('toAccountId'))

                # If SpotTrade, calculate fee and update fee_token_id and fee_amount
                if tx_type == 'SpotTrade':
                    fee, fee_token_id = calculate_fee(transaction)
                    tx_info['fee_amount'] = fee
                    tx_info['fee_token_id'] = fee_token_id
                
                # If transfer, include additional fields
                if tx_type == 'Transfer':
                    tx_info['transfer_token_id'] = transaction.get('token', {}).get('tokenId', 'Unavailable')
                    tx_info['transfer_amount'] = transaction.get('token', {}).get('amount', 'Unavailable')
                    tx_info['transfer_nft_data'] = transaction.get('token', {}).get('nftData', 'Unavailable')

                if tx_type == 'SpotTrade':
                    if transaction['orderA'].get('isAmm') or transaction['orderB'].get('isAmm'):
                        tx_type = 'Swap'
                        tx_info['tx_type'] = tx_type
                    elif not transaction['orderA'].get('isAmm') and not transaction['orderB'].get('isAmm'):
                        tx_type = 'Trade'
                        tx_info['tx_type'] = tx_type
                                                
                transactions_list.append(tx_info)

            return pd.DataFrame(transactions_list)

        else:
            print(f"Error retrieving block {block_id}: {response.status_code}")
            return pd.DataFrame()  # Return an empty DataFrame if there's an error

    def run(self, block_start, batch_size, threads):
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")

        if not check_s3_connection(self.s3_connection):
            raise ConnectionError("S3 is not connected.")
        else:
            print("Successfully connected to S3.")
            
        latest_block = get_latest_block_id()
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")
        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        print(f"Running with start block {block_start} and latest block {latest_block}")
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            
            for current_start in range(block_start, latest_block + 1, batch_size):
                current_end = current_start + batch_size - 1
                if current_end > latest_block:
                    current_end = latest_block

                futures.append(executor.submit(self.fetch_and_process_range_loopring, current_start, current_end, self.chain, self.table_name, self.s3_connection, self.bucket_name, self.db_connector))

            # Wait for all threads to complete and handle any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread raised an exception: {e}")
                    traceback.print_exc()
                    
    def fetch_loopring_data_for_range(self, current_start, current_end):
        print(f"Fetching data for blocks {current_start} to {current_end}...")
        all_blocks_df = pd.DataFrame()

        for block_id in range(current_start, current_end + 1):
            try:
                block_data_df = self.get_block_data(block_id)

                # Skip if block_data_df is empty or all NA
                if block_data_df.empty or block_data_df.isna().all().all():
                    continue

                # Exclude empty or all-NA columns before concatenation
                block_data_df = block_data_df.dropna(how='all', axis=1)
                
                # Concatenate DataFrames
                all_blocks_df = pd.concat([all_blocks_df, block_data_df], ignore_index=True)

            except Exception as e:
                print(f"Error processing block {block_id}: {e}")

            time.sleep(1)  # Pause to avoid hitting API rate limits

        return all_blocks_df

    def fetch_and_process_range_loopring(self, current_start, current_end, chain, table_name, s3_connection, bucket_name, db_connector):
        base_wait_time = 5   # Base wait time in seconds
        while True:
            try:
                # Fetching Loopring block data for the specified range
                df = self.fetch_loopring_data_for_range(current_start, current_end)

                # Check if df is None or empty, and return early without further processing.
                if df is None or df.empty:
                    print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                    return

                # Save data to S3
                save_data_for_range(df, current_start, current_end, chain, s3_connection, bucket_name)

                # Dataframe preparation specific to Loopring
                df_prep = self.prep_dataframe_loopring(df)

                # Remove duplicates and set index
                df_prep.drop_duplicates(subset=['tx_id'], inplace=True)
                df_prep.set_index('tx_id', inplace=True)
                df_prep.index.name = 'tx_id'

                # Upsert data into the database
                try:
                    db_connector.upsert_table(table_name, df_prep, if_exists='update')  # Use DbConnector for upserting data
                    print(f"Data inserted for blocks {current_start} to {current_end} successfully.")
                except Exception as e:
                    print(f"Error inserting data for blocks {current_start} to {current_end}: {e}")
                    raise e
                break  # Break out of the loop on successful execution

            except Exception as e:
                print(f"Error processing blocks {current_start} to {current_end}: {e}")
                base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time)  # handle_retry_exception needs to be defined

    def prep_dataframe_loopring(self, df):    
        # Convert timestamp to datetime
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='ms')

        # Handle bytea data type by ensuring it is prefixed with '\x' instead of '0x'
        for col in ['to_address', 'from_address']:
            if col in df.columns:
                df[col] = df[col].str.replace('0x', '\\x', regex=False)
            else:
                print(f"Column {col} not found in dataframe.")
                
        df.replace({'Unavailable': np.nan, None: np.nan, 'NaN': np.nan, 'nan': np.nan, '': np.nan, 'None': np.nan, '0.0': 0}, inplace=True)

        return df
    
# Helper Functions

# Fees =  (filledS from orderB * feeBips from orderA / 10000) / 10^(decimals of tokenB from orderA)  
# Symbol=  symbol of tokenB from orderA

def calculate_fee(transaction):
    order_a = transaction.get('orderA', {})
    order_b = transaction.get('orderB', {})

    filled_s = float(order_b.get('filledS', 0))  # filledS from orderB
    fee_bips = float(order_a.get('feeBips', 0))  # feeBips from orderA
    token_id = order_a.get('tokenB')  # tokenB from orderA

    token_data = get_token_data(token_id)
    if not token_data:
        return 0, None

    fee = (filled_s * fee_bips / 10000) / (10 ** token_data['decimals'])
    return fee, token_id

def get_token_data(token_id):
    url = f"https://api3.loopring.io/api/v3/exchange/tokens"
    response = requests.get(url)
    if response.status_code == 200:
        tokens = response.json()
        for token in tokens:
            if token.get('tokenId') == token_id:
                return {
                    "symbol": token.get('symbol'),
                    "decimals": token.get('decimals')
                }
    return None

def get_latest_block_id():
    url = "https://api3.loopring.io/api/v3/block/getBlock"
    response = requests.get(url)
    if response.status_code == 200:
        latest_block_data = response.json()
        return latest_block_data['blockId']
    else:
        print("Error retrieving latest block ID:", response.status_code)
        return None