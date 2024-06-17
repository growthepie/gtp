from google.oauth2 import service_account
from googleapiclient.discovery import build

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from src.misc.helper_functions import db_addresses_to_checksummed_addresses, string_addresses_to_checksummed_addresses
import pandas as pd
import time
import json
import os

class Glo:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        pass

    def get_mapping_sheet(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        credentials_info = json.loads(os.getenv('GOOGLE_CREDENTIALS'))
        credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)

        SPREADSHEET_ID = '11GWiQhuGyzYVSKLXxLDyZjU0lFYy8HVxVI7NsSeV50A'
        RANGE_NAME = 'A1:H1000'  # Adjust the range as needed

        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        ## Load data from Google Sheets
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        df = pd.DataFrame(values[1:], columns=values[0])
        print(f"..loaded {len(df)} rows from Google Sheets")
        return df
    
    def clean_mapping_sheet(self, df):
        ## Process dataframe
        df = df.drop(columns=['Date'])

        df = df.rename(columns={
            'Org name1 (ID: id-2b6c4784)': 'org', 
            'Website': 'website', 
            'X': 'twitter', 
            'Wallet address1 (ID: id-962732a7)': 'wallet1', 
            'Wallet address2 (ID: id-0d857e84)': 'wallet2', 
            'Wallet address3 (ID: id-4c34c3f3)': 'wallet3',
            'How would you like us to display your wallet balance? (ID: mc-116efa79)': 'rule'}
            )

        ### move columns wallet1, wallet2, wallet3 to rows
        df = pd.melt(df, id_vars=['org', 'website', 'twitter', 'rule'], value_vars=['wallet1', 'wallet2', 'wallet3'], var_name='wallet', value_name='address')
        df = df[df['address'] != '']
        df = df.drop(columns=['wallet'])
        df = df.drop_duplicates(subset=['org', 'address'])
        print(f"..cleaned up {len(df)} rows")
        return df

    def resolve_ens_mapping_sheet(self, df):
        ## Resolve ENS addresses
        url = 'https://eth.llamarpc.com'
        w3 = Web3(HTTPProvider(url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        w3.is_connected()

        # Check connection
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to the Ethereum node")

        ens = w3.ens

        for i, row in df.iterrows():
            if row['address'][:2] != '0x':
                try:
                    ens_name = row['address']
                    ens_address = ens.address(row['address'])
                    df.loc[i, 'address'] = ens_address
                    print(f"..resolved ENS address {ens_name} to {ens_address}")
                    time.sleep(1)
                except Exception as e:
                    print(e)

        ## Final touches and then merge with holders data
        df = df.drop_duplicates(subset=['org', 'address'])
        df = df[df['address'].str.startswith('0x')]
        df['address'] = df['address'].str.lower()
        print(f"..resolved ENS addresses")
        return df

    def get_glo_holders(self):
        df = self.db_connector.get_glo_holders()
        df = db_addresses_to_checksummed_addresses(df, ['address'])
        df['address'] = df['address'].str.lower()
        df = df[~df['address'].isin(['0x1ed622abdd5ed799a26f8eae45950de4783fc506', '0x0d3b2c24aa5106a78445ab04bab3baacd90230aa'])]
        print(f"..loaded {len(df)} holders from DB")
        return df   

    def run_glo(self, top=30):
        df = self.get_mapping_sheet()
        df = self.clean_mapping_sheet(df)
        df = self.resolve_ens_mapping_sheet(df)
        df_holders = self.get_glo_holders()
        df_full = df_holders.merge(df, how='left', on='address')
        df_full = string_addresses_to_checksummed_addresses(df_full, ['address'])

        df_full['holder'] = df_full['org'].fillna(df_full['address'])
        df_full = df_full.groupby('holder').sum().reset_index()
        df_full = df_full.sort_values('balance', ascending=False).head(top)
        df_full = df_full.drop(columns=['org', 'address'])
        df_full = df_full.replace(0, float("NaN"))
        df_full = df_full.where(pd.notnull(df_full), None)

        return df_full