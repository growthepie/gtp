from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import MetaData, Table, select, and_, or_
from web3 import Web3
from src.adapters.abstract_adapters import AbstractAdapterRaw
import time

class ContractLoader(AbstractAdapterRaw):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Contract_Metadata", adapter_params, db_connector)
        self.chain = adapter_params['chain']
        self.table_name = f'{self.chain}_tx'
        self.days = adapter_params['days']
        self.oli_table = 'oli_tag_mapping'
        self.oli_view = 'vw_oli_labels'
        # Ensure rpc_urls is a list
        if isinstance(adapter_params['rpc_urls'], str):
            self.rpc_urls = [url.strip() for url in adapter_params['rpc_urls'].split(',')]
        else:
            self.rpc_urls = adapter_params['rpc_urls']


    def prep_contract_creations(self, contract_creations):
        df = pd.DataFrame(contract_creations)
        print(f"...extracted {len(df)} NEW contract creations.")

        value_columns = ['deployment_tx', 'deployer_address']
        df['address'] = df['address'].apply(lambda x: ('\\x' + x[2:].upper()) if (pd.notnull(x) and x.startswith('0x')) else x)
        for col in value_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '0x' + x.upper())
            else:
                print(f"Column {col} not found in dataframe.")

        # Melting the DataFrame to a long format
        if set(['address', 'origin_key', 'source', 'added_on']).issubset(df.columns):
            df = df.melt(id_vars=['address', 'origin_key', 'source', 'added_on'], var_name='tag_id', value_name='value')
        else:
            missing_cols = set(['address', 'origin_key', 'source', 'added_on']) - set(df.columns)
            print(f"Missing columns: {', '.join(missing_cols)} in dataframe.")

        primary_keys = ['address', 'origin_key', 'tag_id']
        df.drop_duplicates(subset=primary_keys, inplace=True)
        df.set_index(primary_keys, inplace=True)

        return df

    def extract_raw(self):
        try:
            print(f"...start extracting contract creations TX for {self.chain} and last {self.days} days from our database.")
            contract_creations = self.fetch_contract_creations()
            if not contract_creations:  # Check if contract_creations is empty
                print(f"No contract creations found for chain {self.chain} and last {self.days} days.")
                return         

        except Exception as e:
            raise e
        
        df = self.prep_contract_creations(contract_creations)
        self.db_connector.upsert_table(self.oli_table, df, if_exists='update')
        print(f"Successfully inserted data for {self.chain}")

           
    def fetch_contract_creations(self, allow_partial_insert=True):
        try:
            metadata = MetaData(bind=self.db_connector.engine)
            table = Table(self.table_name, metadata, autoload_with=self.db_connector.engine)
            oli_view = Table(self.oli_view, metadata, autoload_with=self.db_connector.engine)
        except Exception as e:
            print(f"Error reflecting table {self.table_name}: {str(e)}")
            raise

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.days)

        # Fetch transactions within the date range
        query = select([
            table.c.tx_hash,
            table.c.block_number,
            table.c.block_timestamp,
            table.c.from_address
        ]).where(and_(
            table.c.block_timestamp >= start_date,
            or_(
                table.c.to_address == None, 
                table.c.to_address == bytes.fromhex('0000000000000000000000000000000000008006')
            )
        ))

        try:
            with self.db_connector.engine.connect() as connection:
                result = connection.execute(query)
                transactions = result.fetchall()
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return []
        
        print(f"...found {len(transactions)} contract creation transactions within the date range.")
        
        # Fetch existing tx_hashes from oli table
        oli_query = select([
            oli_view.c.deployment_tx
        ]).where(and_(
            oli_view.c.deployment_date >= start_date, 
            oli_view.c.origin_key == self.chain
        ))
        
        try:
            with self.db_connector.engine.connect() as connection:
                result = connection.execute(oli_query)
                existing_tx_hashes = {row['deployment_tx'] for row in result.fetchall() if row['deployment_tx'].startswith('0x')}
        except Exception as e:
            print(f"Error fetching existing tx_hashes: {str(e)}")
            return []
        
        print(f"...already {len(existing_tx_hashes)} contract creations existing in our oli table.")

        # Filter out transactions that are already in oli table
        filtered_transactions = [ tx for tx in transactions if f'0x{tx.tx_hash.hex().upper()}' not in existing_tx_hashes]

        # Check if there are no filtered transactions
        if len(filtered_transactions) == 0:
            return []
        
        contract_creations = []
        for rpc_url in self.rpc_urls:
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            if w3.is_connected():
                for index, tx in enumerate(filtered_transactions):
                    tx_hash = tx.tx_hash.hex() if isinstance(tx.tx_hash, bytes) else tx.tx_hash
                    try:
                        receipt = w3.eth.get_transaction_receipt(tx_hash)
                        if receipt.contractAddress:
                            contract_creations.append({
                                'deployment_tx': tx_hash,
                                'address': receipt.contractAddress,
                                'source': 'metadata_extraction_script',
                                'added_on': datetime.now(timezone.utc),
                                'origin_key': self.chain,
                                'is_contract': True,
                                'deployment_date': tx.block_timestamp,
                                'deployer_address': tx.from_address.hex() if isinstance(tx.from_address, bytes) else tx.from_address,
                            })
                    except Exception as e:
                        print(f"Error fetching receipt for tx_hash {tx_hash}: {str(e)}")
                        time.sleep(2)
                        continue
                    
                    if index % 100 == 0:
                        print(f"...processing transaction {index} out of {len(filtered_transactions)} for {self.chain}.")
                    if index % 1000 == 0  and index > 1 and allow_partial_insert:
                        if len(contract_creations) > 0:
                            df = self.prep_contract_creations(contract_creations)
                            self.db_connector.upsert_table(self.oli_table, df, if_exists='update')
                            print(f"...partially inserted data for {self.chain}")
                            contract_creations = []
                        else:
                            print(f"...seems like all tx_hashes are NON contract creations -- investigate!")
                            print(f'...last tx_hash: {tx_hash}')

                return contract_creations
            else:
                print(f"Failed to connect using RPC URL: {rpc_url}")
        raise Exception("All RPC connections failed.")