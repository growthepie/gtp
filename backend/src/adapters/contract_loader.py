from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import MetaData, Table, select, and_
from web3 import Web3
from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.db_connector import DbConnector

class ContractLoader(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Contract_Metadata", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        self.days = adapter_params['days']
        self.oli_table = 'oli_tag_mapping'
        # Ensure rpc_urls is a list
        if isinstance(adapter_params['rpc_urls'], str):
            self.rpc_urls = [url.strip() for url in adapter_params['rpc_urls'].split(',')]
        else:
            self.rpc_urls = adapter_params['rpc_urls']

    def extract_raw(self):
        try:
            contract_creations = self.fetch_contract_creations()
            # Convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(contract_creations)
            df['deployment_tx'] = df['deployment_tx'].str.replace('0x', '\\x', regex=False)
            df = df.melt(id_vars=['address', 'origin_key', 'source', 'added_on'], var_name='tag_id', value_name='value')
                
        except Exception as e:
            raise e
        
        if df is None or df.empty:
            print(f"No contract creations found for chain {self.chain} and last {self.days} days.")
        else:
            # Specify the primary keys
            primary_keys = ['address', 'origin_key', 'tag_id']
            
            # Drop duplicates based on the primary keys
            df.drop_duplicates(subset=primary_keys, inplace=True)
            
            # Set the primary keys as the index
            df.set_index(primary_keys, inplace=True)
            
        # Upsert data into the database
        try:
            self.db_connector.upsert_table(self.oli_table, df, if_exists='update')
            print("Successfully inserted data")
        except Exception as e:
            raise e
           
    def fetch_contract_creations(self):
        try:
            metadata = MetaData(bind=self.db_connector.engine)
            table = Table(self.table_name, metadata, autoload_with=self.db_connector.engine)
            oli_table = Table(self.oli_table, metadata, autoload_with=self.db_connector.engine)
        except Exception as e:
            print(f"Error reflecting table {self.table_name}: {str(e)}")
            raise

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=self.days)

        # Fetch transactions within the date range
        query = select([
            table.c.tx_hash,
            table.c.block_number,
            table.c.block_timestamp,
            table.c.from_address
        ]).where(and_(
            table.c.block_timestamp >= start_date,
            table.c.block_timestamp <= end_date,
            table.c.to_address == None
        ))

        try:
            with self.db_connector.engine.connect() as connection:
                result = connection.execute(query)
                transactions = result.fetchall()
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return []

        # Fetch existing tx_hashes from oli table
        oli_query = select([oli_table.c.value]).where(oli_table.c.tag_id == 'tx_hash')
        
        try:
            with self.db_connector.engine.connect() as connection:
                existing_tx_hashes = {row.tx_hash for row in connection.execute(oli_query).fetchall()}
        except Exception as e:
            print(f"Error fetching existing tx_hashes: {str(e)}")
            return []

        # Filter out transactions that are already in oli table
        filtered_transactions = [tx for tx in transactions if tx.tx_hash not in existing_tx_hashes]

        contract_creations = []
        for rpc_url in self.rpc_urls:
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            if w3.is_connected():
                for tx in filtered_transactions:
                    tx_hash = tx.tx_hash.hex() if isinstance(tx.tx_hash, bytes) else tx.tx_hash
                    receipt = w3.eth.get_transaction_receipt(tx_hash)
                    if receipt.contractAddress:
                        contract_creations.append({
                            'deployment_tx': tx_hash,
                            'address': receipt.contractAddress,
                            'source': 'metadata_extraction_script',
                            'added_on': datetime.utcnow(),
                            'origin_key': self.chain,
                            'is_contract': True,
                            'deployment_date': tx.block_timestamp,
                            'deployer_address': tx.from_address.hex() if isinstance(tx.from_address, bytes) else tx.from_address,
                        })
                return contract_creations
            else:
                print(f"Failed to connect using RPC URL: {rpc_url}")
        raise Exception("All RPC connections failed.")