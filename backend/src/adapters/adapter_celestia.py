from src.adapters.abstract_adapters import AbstractAdapterRaw
import traceback
from src.adapters.funcs_rps_utils import *
import requests
import base64

class AdapterCelestia(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Celestia", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.rpc_list = adapter_params['rpc_list']
        self.table_name = f'{self.chain}_tx'   
        self.db_connector = db_connector
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
        
    def extract_raw(self, load_params:dict):
        self.block_start = load_params['block_start']
        self.batch_size = load_params['batch_size']
        self.run(self.block_start, self.batch_size)
        print(f"FINISHED loading raw tx data for {self.chain}.")
        
    def run(self, block_start, batch_size):
        latest_block = self.get_latest_block()
        if latest_block is None:
            print("Could not fetch the latest block.")
            raise ValueError("Could not fetch the latest block.")
        if block_start == 'auto':
            block_start = self.db_connector.get_max_block(self.table_name)  
        else:
            block_start = int(block_start)

        print(f"Running with start block {block_start} and latest block {latest_block}")
        block_start = int(block_start)
        latest_block = int(latest_block)
        batch_size = int(batch_size)
        for current_start in range(block_start, latest_block + 1, batch_size):
            current_end = current_start + batch_size - 1
            if current_end > latest_block:
                current_end = latest_block

            try:
                self.fetch_and_process_range(current_start, current_end, self.chain, self.table_name, self.s3_connection, self.bucket_name, self.db_connector)
            except Exception as e:
                print(f"Error processing range {current_start}-{current_end}: {e}")
                traceback.print_exc()

    def fetch_data_for_range(self, block_start, block_end):
        df = pd.DataFrame()
        for block_number in range(block_start, block_end + 1):
            print(f"Fetching data for block {block_number}")
            block_df = self.retrieve_block_data(block_number)
            df = pd.concat([df, block_df], ignore_index=True)
        return df

    def request_rpc(self, payload, headers):
        for rpc_endpoint in self.rpc_list:
            try:
                response = requests.post(rpc_endpoint, json=payload, headers=headers)
                if response and response.status_code == 200:
                    return response
            except Exception as e:
                print(f"RPC failed at {rpc_endpoint} with error: {e}")
        print("All RPC endpoints failed.")
        return None

    def get_latest_block(self):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "header",
            "params": {}
        }
        response = self.request_rpc(payload, headers)
        if response:
            block_number = response.json()['result']['header']['height']
            return block_number
        return None

    def retrieve_block_data(self, block_number):
        df = pd.DataFrame()
        page = 1
        tx_search = self.fetch_block_transaction_details(block_number, page)
        while tx_search and tx_search['result']['txs']:
            # Append the data frame with new data
            df = pd.concat([df, self.prep_dataframe_celestia(tx_search)], ignore_index=True)
            if len(tx_search['result']['txs']) < 100:
                break  # No more pages to fetch
            page += 1
            tx_search = self.fetch_block_transaction_details(block_number, page)
        return df
     
    def get_block_timestamp(self, block_number):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "method": "block",
            "params": [str(block_number)],
            "id": 1
        }
        response = self.request_rpc(payload, headers)
        if response:
            return response.json()['result']['block']['header']['time']
        print(f"Failed to fetch block timestamp for block {block_number}.")
        return None

    def fetch_block_transaction_details(self, block_number, page=1):
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "method": "tx_search",
            "params": {
                "query": f"tx.height={str(block_number)}",
                "prove": True,
                "page": f'{page}',
                "per_page": '100',
                "order_by": "asc",
                "match_events": True
            },
            "id": 1
        }
        response = self.request_rpc(payload, headers)
        if response:
            return response.json()
        print("Failed to fetch transaction details for block.")
        return None

    def prep_dataframe_celestia(self, tx):
        if tx['result']['txs'] == None or tx['result']['txs'] == []:
            print('No transactions found in this block!')
            return pd.DataFrame()

        data = []
        txs_decoded = decode_base64(tx['result']['txs'])
        block = txs_decoded[0]['height']
        timestamp = self.get_block_timestamp(block)
        for trx in txs_decoded:
            row = {}
            row['block_timestamp'] = timestamp
            row['block_number'] = block
            
            # Format tx_hash for bytea storage in PostgreSQL
            if trx['hash'].startswith('0x'):
                row['tx_hash'] = '\\x' + trx['hash'][2:]  # Remove '0x' and prepend '\\x'
            else:
                row['tx_hash'] = '\\x' + trx['hash']  # Prepend '\\x' directly if there's no '0x'
            
            row['gas_wanted'] = int(trx['tx_result']['gas_wanted'])
            row['gas_used'] = int(trx['tx_result']['gas_used'])
            attributes = [i['attributes'] for i in trx['tx_result']['events']]
            for a in attributes:
                for attr in a:
                    key = attr['key']
                    value = attr['value']
                    if key in ['spender', 'sender', 'receiver', 'recipient']:
                        row[key] = value
                    elif key == 'acc_seq':
                        # Check if there is a '/' and split to get the number after it
                        if '/' in value:
                            row[key] = value.split('/')[1]
                        else:
                            row[key] = value
                    elif key == 'fee':
                        if value is not None:
                            row[key] = int(value.replace('utia', ''))
                            row['fee_payer'] = a[1]['value']
                        else:
                            print(f"Warning: 'value' is None for key 'fee' in attributes {attributes}")
                            row[key] = 0
                    elif key == 'action':
                        row[key] = value[1:]
                    elif key == 'signature':
                        row[key] = value
                    elif key == 'blob_sizes':
                        row[key] = [int(i) for i in value[1:-1].split(',')]
                        row['namespaces'] = [i[1:-1] for i in a[1]['value'][1:-1].split(',')]
                        row['signer'] = a[2]['value'][1:-1]

            data.append(row)

        return pd.DataFrame(data)

    def fetch_and_process_range(self, current_start, current_end, chain, table_name, s3_connection, bucket_name, db_connector):
        base_wait_time = 5   # Base wait time in seconds
        while True:
            try:
                # Fetching Celestia block data for the specified range
                df = self.fetch_data_for_range(current_start, current_end)

                # Check if df is None or empty, and return early without further processing.
                if df is None or df.empty:
                    print(f"Skipping blocks {current_start} to {current_end} due to no data.")
                    return

                # Save data to S3
                save_data_for_range(df, current_start, current_end, chain, s3_connection, bucket_name)

                # Remove duplicates and set index
                df.drop_duplicates(subset=['tx_hash'], inplace=True)
                df.set_index('tx_hash', inplace=True)
                df.index.name = 'tx_hash'

                # Upsert data into the database
                try:
                    db_connector.upsert_table(table_name, df, if_exists='update')
                    print(f"Data inserted for blocks {current_start} to {current_end} successfully.")
                except Exception as e:
                    print(f"Error inserting data for blocks {current_start} to {current_end}: {e}")
                    raise e
                break  # Break out of the loop on successful execution

            except Exception as e:
                print(f"Error processing blocks {current_start} to {current_end}: {e}")
                base_wait_time = handle_retry_exception(current_start, current_end, base_wait_time)

def decode_base64(element):
    if isinstance(element, dict):
        return {key: decode_base64(value) for key, value in element.items()}
    elif isinstance(element, list):
        return [decode_base64(item) for item in element]
    elif isinstance(element, str):
        try:
            decoded_bytes = base64.b64decode(element, validate=True)
            decoded_str = decoded_bytes.decode('utf-8')
            return decoded_str
        except Exception:
            return element
    else:
        return element  