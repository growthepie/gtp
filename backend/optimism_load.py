import os
import pandas as pd
import concurrent
from concurrent.futures import ThreadPoolExecutor
import json
from src.misc.helper_functions import api_post_call, dataframe_to_s3
from datetime import datetime
from src.db_connector import DbConnector
import time

#ankr_api = os.getenv("ANKR_API")
# ankr_api = os.getenv("ANKR_API")
# url = f"https://rpc.ankr.com/optimism/{ankr_api}/"

alchemy_api = os.getenv("ALCHEMY_API")
url = f'https://opt-mainnet.g.alchemy.com/v2/{alchemy_api}'

def createPayloadGetBlockByNumber(block_numbers:list):
    payload = []
    for i, block_number in enumerate(block_numbers):
        payload.append({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [str(hex(block_number)), True],
            "id": i+1
        })
    return payload


def createPayloadGetTxReceipt(tx_hashs:list):
    payload = []
    for i, tx_hash in enumerate(tx_hashs):
        payload.append({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": i+1
        })
    return payload

def getDataframeWithTransactionsByBlockNumber(blocknumber:int):
    block_number_hex = hex(blocknumber)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [str(block_number_hex), True],
        "id": 1
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = api_post_call(url, payload=json.dumps(payload), header=headers)
    df = pd.DataFrame(response['result']['transactions'])


    ## convert timestamp from hex to datetime in utc
    timestamp = datetime.utcfromtimestamp(int(response['result']['timestamp'], 16))
    df['block_timestamp'] = timestamp

    return df

def getDataframeWithTransactionsByBlockNumberBatch(block_start:int, batch_size:int=100):
    block_numbers = list(range(block_start, block_start + batch_size))
    payload = createPayloadGetBlockByNumber(block_numbers)

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = api_post_call(url, payload=json.dumps(payload), header=headers)
    df = pd.DataFrame()
    for r in response:
        df_temp = pd.DataFrame(r['result']['transactions'])
        ## convert timestamp from hex to datetime in utc
        timestamp = datetime.utcfromtimestamp(int(r['result']['timestamp'], 16))
        df_temp['block_timestamp'] = timestamp
        df = pd.concat([df, df_temp])

    return df

def getTransactionReceipt(tx_hash:str):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash],
        "id": 1
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = api_post_call(url, payload=json.dumps(payload), header=headers)
    return response['result']

def getTransactionReceiptBatch(tx_hashs:list):
    payload = createPayloadGetTxReceipt(tx_hashs)
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = api_post_call(url, payload=json.dumps(payload), header=headers)
    return response


def getDataframeWithTxReceiptsByBlockNumber(blocknumber:int):
    all_tx_receipts = []
    dfBlock = getDataframeWithTransactionsByBlockNumber(blocknumber)
    dfBlock = dfBlock[['block_timestamp', 'hash', 'gas', 'value', 'input', 'nonce', 'v', 'r', 's']]
    tx_hashes = dfBlock['hash'].tolist()

    for tx_hash in tx_hashes:
        response = getTransactionReceipt(tx_hash)
        all_tx_receipts.append(response)

    df = pd.DataFrame(all_tx_receipts)
    df = df.merge(dfBlock, left_on='transactionHash', right_on='hash', how='left')
    return df


def getDataframeWithTxReceiptsByBlockNumberBatch(block_start:int, batch_size:int=100):
    all_tx_receipts = []
    dfBlock = getDataframeWithTransactionsByBlockNumberBatch(block_start, batch_size)
    dfBlock = dfBlock[['block_timestamp', 'hash', 'gas', 'value', 'input', 'nonce', 'v', 'r', 's']]
    tx_hashes = dfBlock['hash'].tolist()
    #print(f"Loaded {len(tx_hashes)} tx hashes.")

    for i in range (0, len(tx_hashes), batch_size):
        #print(f"Getting tx receipts for batch {i} - {i+batch_size}...")
        response_list = getTransactionReceiptBatch(tx_hashes[i:i+batch_size])
        all_tx_receipts.extend(response_list)

    #print(f"Finished getting tx receipts for {len(tx_hashes)} tx hashes. Now preparing dataframe...")
    df = pd.DataFrame()
    for tx in all_tx_receipts:        
        tx['result'].pop('logs', None)
        df_temp = pd.DataFrame(tx['result'], index=[0])
        df = pd.concat([df, df_temp])    

    #print(f"Loaded {df.shape[0]} tx receipts and {dfBlock.shape[0]} blocks. Now merging 2 dataframes...")
    df = df.merge(dfBlock, left_on='transactionHash', right_on='hash', how='left')

    return df

def getTxDataForBlockRange(block_start:int, block_end:int, threads:int=50):
    print(f"Getting data for block range {block_start} - {block_end} using {threads} threads...")
    blocks = range(block_start, block_end)

    df = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_url = {executor.submit(getDataframeWithTxReceiptsByBlockNumber, block) for block in blocks}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                df = pd.concat([df, future.result()])
            except Exception as e:
                print('Looks like something went wrong:', e)
                raise ValueError(f"Error in retrieving future")
    return df

def getTxDataForBlockRangeBatch(block_start:int, block_end:int, threads:int=50, batch_size:int=100):
    print(f"Getting data for block range {block_start} - {block_end} using {threads} threads and batch_size of {batch_size}...")
    blocks = range(block_start, block_end, batch_size)

    df = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_url = {executor.submit(getDataframeWithTxReceiptsByBlockNumberBatch, block_start, batch_size) for block_start in blocks}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                df = pd.concat([df, future.result()])
            except Exception as e:
                print('Looks like something went wrong:', e)
                raise ValueError(f"Error in retrieving future")
    return df

def prep_dataframe_ankr(df):
    # Lower case column names
    df.columns = df.columns.str.lower()

    

    # Columns to be used from the dataframe
    if 'l1gasused' in df.columns:
        cols = ['blocknumber', 'block_timestamp', 'hash', 'from', 'to', 'status', 'value', 'gas', 'gasused', 'effectivegasprice', 'l1gasused', 'l1gasprice', 'l1feescalar', 'input']
    else:
        cols = ['blocknumber', 'block_timestamp', 'hash', 'from', 'to', 'status', 'value', 'gas', 'gasused', 'effectivegasprice', 'input']

    # Filter the dataframe to only include the above columns
    df = df.loc[:, cols]

    # Rename columns
    df.rename(columns={'blocknumber': "block_number", "hash": "tx_hash", "from": "from_address", "to": "to_address", "gas": "gas_limit", "gasused": "gas_used", "effectivegasprice": "gas_price", "l1gasused": "l1_gas_used", "l1gasprice": "l1_gas_price", "l1feescalar": "l1_fee_scalar", "input": "input_data"}, inplace=True, errors="ignore")

    # Handle bytea data type
    for col in ['tx_hash', 'to_address', 'from_address']:
        if col in df.columns:
            df[col] = df[col].str.replace('0x', '\\x', regex=False)
        else:
            print(f"Column {col} not found in dataframe.")

    # gas_price column in eth
    df['gas_price'] = df['gas_price'].astype(float) / 1e9

    # l1_gas_price column in eth
    if 'l1_gas_price' in df.columns:
        df['l1_gas_price'] = df['l1_gas_price'].astype(float) / 1e9
    else:
        df['l1_gas_price'] = 0

    ## l1_fee_scalar column as 
    if 'l1_fee_scalar' in df.columns:
        df['l1_fee_scalar'] = df['l1_fee_scalar'].astype(float)
    else:
        df['l1_fee_scalar'] = 0

    if 'l1_gas_used' not in df.columns:
        df['l1_gas_used'] = 0

    # tx_fee (gas_price * gas_used) + (l1_gas_used * l1_gas_price * l1_fee_scalar)

    df['tx_fee'] = ((df['gas_price'] * df['gas_used']) + (df['l1_gas_used'] * df['l1_gas_price'] * df['l1_fee_scalar']))

    # Add empty_input column True when input is empty or 0x then true else false
    df['empty_input'] = df['input_data'].apply(lambda x: True if (x == '0x' or x == '') else False)

    # Drop the 'input' column
    df = df.drop(columns=['input_data'])

    return df


## create main

def main():
    db_connector = DbConnector()

    # block_start = 103417407 ## original start
    table_name = 'new_optimism_tx'
    block_start = db_connector.get_max_block(table_name)            
    print(f"Starting from block {block_start}...")

    error_count = 0

    while block_start < 107600000:
        try:
            # ## with batch (ankr)
            # block_end = block_start + 2000
            # df = getTxDataForBlockRangeBatch(block_start, block_end, 40, 50)

            ## without batch (alchemy, because of CUPS limit)
            block_end = block_start + 500
            df = getTxDataForBlockRange(block_start, block_end, 8)

            ## replace nan with 0x0 in columns ['l1GasUsed', 'l1GasPrice', 'l1Fee']
            for col in ['l1GasUsed', 'l1GasPrice', 'l1Fee']:
                if col in df.columns:
                    df[col].fillna('0x0', inplace=True)

            ## replace nan with 0 in column l1FeeScalar
            if 'l1FeeScalar' in df.columns:
                df['l1FeeScalar'].fillna('0', inplace=True)

            ## convert hex columns to decimal
            for col in ['blockNumber','cumulativeGasUsed', 'effectiveGasPrice', 'gasUsed', 'status', 'l1GasUsed', 'value', 'l1GasPrice', 'l1Fee', 'gas']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: int(x, 16))

            # gas_price column in eth
            df['effectiveGasPrice'] = df['effectiveGasPrice'].astype(float) / 1e9

            # l1_gas_price column in eth
            if 'l1GasPrice' in df.columns:
                df['l1GasPrice'] = df['l1GasPrice'].astype(float) / 1e9

            # value column in eth
            df['value'] = df['value'].astype(float) / 1e18

            ## upload to S3
            file_name = f"optimism_tx_{df.blockNumber.min()}-{df.blockNumber.max()}_alchemy"

            ## upload to s3
            dataframe_to_s3(f'optimism/{file_name}', df)

            ## do other prep
            df = prep_dataframe_ankr(df)

            ## upsert data to db
            df.drop_duplicates(subset=['tx_hash'], inplace=True)
            df.set_index('tx_hash', inplace=True)
            db_connector.upsert_table(table_name, df)
            print(f"...upserted {df.shape[0]} rows to {table_name} table")

            block_start = block_end
        except Exception as e:
            print(e)
            print(f"Error in block range {block_start} - {block_end}. Start over in 5s")
            error_count += 1
            if error_count > 20:
                print("Too many errors. Stopping...")
                break
            time.sleep(5)

if __name__ == "__main__":
    main()