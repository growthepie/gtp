import time
import pandas as pd
from datetime import datetime
import os
import requests

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract, send_discord_message

class AdapterLabelPool(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLI Label Pool", adapter_params, db_connector)
        self.base_url = 'https://base.easscan.org/graphql'
        self.base_sepolia_url = 'https://base-sepolia.easscan.org/graphql' # testnet
        self.schemaId = '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68' # OLI v1.0.0 schema
        self.webhook = os.getenv('DISCORD_CONTRACTS')
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        optional: time (int), from when to load data
    """
    def extract(self, load_params:dict = None):
        if load_params is None:
            #df = self.db_connector.
            load_params = {'time': int(self.db_connector.get_max_value('oli_label_pool_bronze', 'time_created'))}
        # get latest new attestations
        df_new = self.get_latest_new_attestations(load_params)
        # get latest revoked attestations
        df_rev = self.get_latest_revoked_attestations(load_params)
        # merge new and revoked attestations
        df = pd.concat([df_new, df_rev])
        # remove duplicates
        df = df.drop_duplicates(subset=['id'], keep='last')
        print_extract(self.name, load_params, df.shape)
        if len(df) > 0:
            send_discord_message(f"{len(df)} new labels were submitted to the OLI Label Pool 🎉", self.webhook)
        return df

    def load(self, df:pd.DataFrame):
        if df.empty == False:
            # convert id, attester, recipient and tx_id columns to bytea
            df['id'] = df['id'].apply(lambda x: '\\x' + x[2:])
            df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])
            df['recipient'] = df['recipient'].apply(lambda x: '\\x' + x[2:])
            df['tx_id'] = df['tx_id'].apply(lambda x: '\\x' + x[2:] if isinstance(x, str) and x.startswith('0x') else '\\x')
            # set id as index
            df = df.set_index('id')
            # load df into db, updated row if id already exists
            self.db_connector.upsert_table('oli_label_pool_bronze', df, if_exists='update')
        print_load(self.name, {}, df.shape)

    ## ----------------- Helper functions --------------------

    def get_latest_new_attestations(self, load_params):
        # get latest attestations
        j = self.graphql_query_attestations(self.schemaId, 999999, load_params['time'], 0)
        if len(j['data']['attestations']) == 0:
            return pd.DataFrame()
        # create df from json
        df = pd.DataFrame(j['data']['attestations'])
        # rename columns & sort
        df = df[['id', 'attester', 'recipient', 'isOffchain', 'revoked', 'ipfsHash', 'txid', 'decodedDataJson', 'time', 'timeCreated', 'revocationTime']]
        df = df.rename(columns={'isOffchain': 'is_offchain', 'ipfsHash': 'ipfs_hash', 'txid': 'tx_id', 'decodedDataJson': 'decoded_data_json', 'timeCreated': 'time_created', 'revocationTime': 'revocation_time'})
        # return df with new attestations
        return df

    def get_latest_revoked_attestations(self, load_params):
        # get latest revoked attestations
        j = self.graphql_query_attestations(self.schemaId, 999999, 0, load_params['time'])
        if len(j['data']['attestations']) == 0:
            return pd.DataFrame()
        # create df from json
        df = pd.DataFrame(j['data']['attestations'])
        # rename columns & sort
        df = df[['id', 'attester', 'recipient', 'isOffchain', 'revoked', 'ipfsHash', 'txid', 'decodedDataJson', 'time', 'timeCreated', 'revocationTime']]
        df = df.rename(columns={'isOffchain': 'is_offchain', 'ipfsHash': 'ipfs_hash', 'txid': 'tx_id', 'decodedDataJson': 'decoded_data_json', 'timeCreated': 'time_created', 'revocationTime': 'revocation_time'})
        # return df with revoked attestations
        return df
        
    def graphql_query_attestations(self, schemaId, count, timeCreated, revocationTime):
        query = """
            query Attestations($take: Int, $where: AttestationWhereInput, $orderBy: [AttestationOrderByWithRelationInput!]) {
            attestations(take: $take, where: $where, orderBy: $orderBy) {
                attester
                decodedDataJson
                expirationTime
                id
                ipfsHash
                isOffchain
                recipient
                refUID
                revocable
                revocationTime
                revoked
                time
                timeCreated
                txid
            }
            }
        """
            
        variables = {
            "take": count,
            "where": {
                "schemaId": {
                    "equals": schemaId
                },
                "timeCreated": {
                    "gt": timeCreated
                },
                "revocationTime": {
                    "gte": revocationTime
                }
            },
            "orderBy": [
                {
                "timeCreated": "desc"
                }
            ]
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.post(self.base_url, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"GraphQL query failed with status code {response.status_code}: {response.text}")

