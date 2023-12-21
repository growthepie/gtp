import requests
import pandas as pd
from adapter_utils import create_db_engine, load_environment
import numpy as np
from pangres import upsert
import time
   
# Helper Functions
def get_account_address(account_id):
    url = f"https://api3.loopring.io/api/v3/account?accountId={account_id}"
    response = requests.get(url)
    if response.status_code == 200:
        account_data = response.json()
        return account_data.get('owner')
    else:
        return None
    
def get_block_data(block_id):
    print(f"Retrieving block {block_id}...")
    url = f"https://api3.loopring.io/api/v3/block/getBlock?id={block_id}"
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
                'from_address': 'Unavailable',  # Placeholder for from_address
                'to_address': 'Unavailable',    # Placeholder for to_address
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
                    tx_info['from_address'] = get_account_address(from_account_id)
            elif 'owner' in transaction:
                tx_info['from_address'] = transaction.get('owner')
            elif 'accountId' in transaction:
                tx_info['from_address'] = get_account_address(transaction.get('accountId'))

            to_address = transaction.get('toAddress') or transaction.get('toAccountAddress')
            if to_address:
                tx_info['to_address'] = to_address
            elif 'toAccountId' in transaction:
                tx_info['to_address'] = get_account_address(transaction.get('toAccountId'))

            # If SpotTrade, calculate fee and update fee_token_id and fee_amount
            if tx_type == 'SpotTrade':
                fee, fee_token_id = calculate_fee(transaction)
                tx_info['fee_amount'] = fee
                tx_info['fee_token_id'] = fee_token_id

            # Handle other transaction types as needed

            transactions_list.append(tx_info)

        return pd.DataFrame(transactions_list)

    else:
        print(f"Error retrieving block {block_id}: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error
    
def is_nft(nft_data):
    return bool(nft_data)

def calculate_fee(transaction):
    order_a = transaction.get('orderA', {})
    order_b = transaction.get('orderB', {})

    token_a_is_nft = is_nft(order_a.get('nftData'))
    token_b_is_nft = is_nft(order_b.get('nftData'))

    amount = 0
    fee_token_id = None

    # Determining the fee token and the relevant amount
    if token_b_is_nft and not token_a_is_nft:
        amount = float(order_a.get('amountS', 0))
        fee_token_id = order_a.get('tokenS')
    elif not token_b_is_nft:
        amount = float(order_a.get('amountB', 0))
        fee_token_id = order_a.get('tokenB')

    # If both tokens are NFTs, no fee can be directly paid
    if token_b_is_nft and token_a_is_nft:
        return 0, None

    # Calculate the fee using basis points
    fee_bips = float(order_a.get('feeBips', 0))
    fee = amount * (fee_bips / 10000) 

    return fee, fee_token_id

def prep_dataframe_loopring(df):    
    # Convert timestamp to datetime
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='ms')

    # Handle bytea data type by ensuring it is prefixed with '\x' instead of '0x'
    for col in ['to_address', 'from_address']:
        if col in df.columns:
            df[col] = df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")
            
    df.replace({'Unavailable': np.nan, None: np.nan, 'NaN': np.nan, 'nan': np.nan, '': np.nan}, inplace=True)

    return df

def insert_into_db(df, engine, table_name):
    # Set the DataFrame's index to 'tx_hash' (your primary key)
    df.set_index('tx_id', inplace=True)
    df.index.name = 'tx_id'
    
    # Insert data into database
    print(f"Inserting data into table {table_name}...")
    try:
        upsert(engine, df, table_name, if_row_exists='update')
    except Exception as e:
        print(f"Error inserting data into table {table_name}.")
        print(e)
   
def get_latest_block_id():
    url = "https://api3.loopring.io/api/v3/block/getBlock"
    response = requests.get(url)
    if response.status_code == 200:
        latest_block_data = response.json()
        return latest_block_data['blockId']
    else:
        print("Error retrieving latest block ID:", response.status_code)
        return None
    
def load_data_to_db(first_block_id, latest_block_id, db_user, db_password, db_host, db_port, db_name):
    engine = create_db_engine(db_user, db_password, db_host, db_port, db_name)
    table_name = 'loopring_tx'
    
    for block_id in range(first_block_id, latest_block_id):
        try:
            block_data_df = get_block_data(block_id)
            if not block_data_df.empty:
                filtered_df = prep_dataframe_loopring(block_data_df)
                insert_into_db(filtered_df, engine, table_name)
            print(f"Successfully inserted data for block {block_id}")
        except Exception as e:
            print(f"Error processing block {block_id}: {e}")
        
        time.sleep(1)  # Pause to avoid hitting API rate limits

def main():
    # Load environment variables and get latest block ID
    db_name, db_user, db_password, db_host, db_port = load_environment()
    latest_block_id = get_latest_block_id()
    first_block_id = 0

    # Load data to database
    load_data_to_db(first_block_id, latest_block_id, db_user, db_password, db_host, db_port, db_name)
    
if __name__ == "__main__":
    main()