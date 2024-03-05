from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.adapters.adapter_utils import *
import pandas as pd
import json
import requests

class AdapterRhino(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Rhino", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.json_endpoint = adapter_params['json_endpoint']
        self.table_name = f'{self.chain}_tx'
        self.db_connector = db_connector
        
        # Initialize S3 connection
        self.s3_connection, self.bucket_name = connect_to_s3()
        
    def extract_raw(self):
        self.run()
        print(f"FINISHED loading raw tx data for {self.chain}.")

    def run(self):
        if not check_db_connection(self.db_connector):
            raise ConnectionError("Database is not connected.")
        else:
            print("Successfully connected to database.")
        
        if not check_s3_connection(self.s3_connection):
            raise ConnectionError("S3 is not connected.")
        else:
            print("Successfully connected to S3.")
        
        json_data = self.download_file_and_load_into_df()
        df = self.prepare_data_from_json(json_data)
        
        try:
            self.db_connector.upsert_table(self.table_name, df, if_exists='update')  # Use DbConnector for upserting data
        except Exception as e:
            print(f"Error inserting data into table {self.table_name}: {e}")
            raise e        

    def download_file_and_load_into_df(self):
        # Download the file
        local_filename = self.json_endpoint.split('/')[-1]
        with requests.get(self.json_endpoint, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        # Assuming the file is a JSON, load it into a DataFrame
        json_data_df = pd.read_json(local_filename)
        save_to_s3(json_data_df, self.chain, self.s3_connection, self.bucket_name)
        os.remove(local_filename)
        
        return json_data_df
        
    def prepare_data_from_json(self, df):        
        # Rename columns to match the required mapping
        df.rename(columns={
            'address': 'from_address',
            'token': 'token',
            'txnType': 'tx_type',
            'amount': 'amount',
            'createdAt': 'block_timestamp',
            'txHash': 'tx_hash',
            'chain': 'chain'
        }, inplace=True)
        
        df = df[['from_address', 'token', 'tx_type', 'amount', 'block_timestamp', 'tx_hash', 'chain']] 
        
        # Remove entries where 'tx_hash' is None
        df = df.dropna(subset=['tx_hash'])

        # Remove duplicates without using inplace=True
        df = df.drop_duplicates(subset=['tx_hash'])

        # Handle bytea data type
        for col in ['from_address']:
            if col in df.columns:
                df[col] = df[col].str.replace('0x', '\\x', regex=False)
            else:
                print(f"Column {col} not found in dataframe.")

        df.set_index('tx_hash', inplace=True)
        
        return df