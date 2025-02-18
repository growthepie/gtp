import csv
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any
import time

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterBlockscout(AbstractAdapter):

    @dataclass
    class Contract:
        address: str 
        origin_key: str 
        contract_name: str = None 
        deployment_tx: str = None 
        deployer_address: str = None 
        deployment_date: str = None
        source_code_verified: str = None 
        is_proxy: bool = None 
        is_contract: bool = None 
        is_eoa: bool = None 
        erc20_name: str = None 
        erc20_symbol: str = None 
        erc20_decimals: int = None 
        erc721_name: str = None
        erc721_symbol: str = None
        erc1155_name: str = None
        erc1155_symbol: str = None
        # we can get the factory contract address from deployment transaction
        is_factory_contract: bool = None

    @dataclass
    class ProcessingStats:
        total_contracts_input: int = 0
        count_contracts: int = 0
        count_contract_name: int = 0
        count_deployment_tx: int = 0
        count_deployer_address: int = 0
        count_deployment_date: int = 0
        count_source_code_verified: int = 0
        count_is_proxy: int = 0
        count_is_contract: int = 0
        count_is_eoa: int = 0
        count_erc20_name: int = 0
        count_erc20_symbol: int = 0
        count_erc20_decimals: int = 0
        count_erc721_name: int = 0
        count_erc721_symbol: int = 0
        count_erc1155_name: int = 0
        count_erc1155_symbol: int = 0
        count_is_factory_contract: int = 0
        elapsed_time: str = ""

    # adapter_params is empty json {}
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Blockscout", adapter_params, db_connector)
        main_conf = get_main_config()
        self.projects = [chain for chain in main_conf if chain.aliases_blockscout_url is not None]
        print_init(self.name, self.adapter_params)

    def extract(self, load_params:dict):
        # get most used contracts from db
        self.load_params = load_params
        df = self.db_connector.get_most_used_contracts(load_params["number_of_contracts"], load_params["origin_key"], load_params["days_back"])
        df['address'] = df['address'].apply(lambda x: '0x' + ''.join([byte.hex() for byte in x]))
        c = self.turn_df_into_list_of_contracts(df)
        c = self.process_contracts(c)
        if len(c) == 0:
            print(f"No contracts labelled for {load_params['origin_key']}")
            return pd.DataFrame()
        df = self.turn_list_of_contracts_into_df(c)
        df['deployment_date'] = df['deployment_date'].apply(lambda x: x.replace('T', ' ').replace('.000000Z', '') if x is not None else x)
        df = df.rename(columns={'erc20_symbol': 'erc20.symbol', 'erc20_decimals': 'erc20.decimals'})
        # print extract
        print_extract(self.name, load_params, df.shape)
        return df

    def load(self, df:pd.DataFrame):
        if df.empty:
            print(f"No data to load")
            return
        # replace 0x with \x in address column
        df['address'] = df['address'].apply(lambda x: x.replace('0x', '\\x'))
        # make sure the name is not longer than 40 characters
        df['contract_name'] = df['contract_name'].apply(lambda x: x[:40] if x is not None else x)
        # melt all columns apart from address and origin_key into tag_id and value
        df = df.melt(id_vars=['address', 'origin_key'], var_name='tag_id', value_name='value')
        # remove duplicates and any row where value = None
        df = df[~df['value'].isnull()]
        df = df.drop_duplicates(subset=['address', 'origin_key', 'tag_id'])
        # set address, origin_key and tag_id as index
        df = df.set_index(['address', 'origin_key', 'tag_id'])
        # add source and time now
        df['source'] = "blockscout"
        df['added_on'] = time.strftime('%Y-%m-%d %H:%M:%S')
        # upsert all rows into db
        table_name = 'oli_tag_mapping'
        n = self.db_connector.upsert_table(table_name, df, if_exists='ignore')
        # print load
        print_load(self.name, n, table_name)


    ## ----------------- Helper functions --------------------

    def turn_df_into_list_of_contracts(self, df: pd.DataFrame) -> List[Contract]:
        contracts = []
        for index, row in df.iterrows():
            if isinstance(row["address"], memoryview):
                address = '0x' + row["address"].hex()
            else:
                if not row["address"].startswith('0x'):
                    address = '0x' + address
                else:
                    address = row["address"]
            contract = AdapterBlockscout.Contract(
                address = address,
                origin_key = row['origin_key']
            )
            contracts.append(contract)
        return contracts

    def turn_list_of_contracts_into_df(self, contracts: List[Contract]) -> pd.DataFrame:
        data = []
        for contract in contracts:
            data.append(asdict(contract))
        return pd.DataFrame(data)

    def fetch_blockscout_data(self, endpoint: str, address: str):
        url = endpoint + address
        timeout = 10  # Set timeout to 10 seconds

        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise ValueError("Contract not found")
            else:
                if response.text.startswith("<!DOCTYPE html>"):
                    print("Received HTML response, most likely the API is down.")
                    raise ValueError("Received HTML response")
                else:
                    print(f"Failed to fetch data from Blockscout: {response.text}")
                    raise ValueError(f"Unexpected response: {response.status_code}")
        except requests.Timeout:
            print(f"Request timed out after {timeout} seconds.")
            raise TimeoutError("API request timed out")
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            raise

    def map_blockscout_to_open_labels(self, contract: Contract, address_json: json) -> Contract:
    
        keys_address = address_json.keys()
        contracts = []

        # if token, then map name from there, else from contract
        try:
            contract.contract_name = address_json["token"]["name"]
            if address_json["token"]["type"] == "ERC-20":
                contract.erc20_symbol = address_json["token"]["symbol"]
                contract.erc20_decimals = address_json["token"]["decimals"]
            elif address_json["token"]["type"] == "ERC-721":
                contract.erc721_name = address_json["token"]["name"]
                contract.erc721_symbol = address_json["token"]["symbol"]
            elif address_json["token"]["type"] == "ERC-1155":
                contract.erc1155_name = address_json["token"]["name"]
                contract.erc1155_symbol = address_json["token"]["symbol"]
        except:
            if 'name' in keys_address:
                if address_json["name"] != None:
                    contract.contract_name = address_json["name"]

        # map is_proxy if any proxy_type is given
        if 'proxy_type' in keys_address:
            if address_json["proxy_type"] in ["eip1167", "eip1967", "eip2535", "basic_implementation"]:
                contract.is_proxy = True
        
        # map is_contract & is_eoa
        if 'is_contract' in keys_address:
            if address_json["is_contract"] != None:
                contract.is_contract = bool(address_json["is_contract"])
                contract.is_eoa = not contract.is_contract

        # map creation_tx_hash if key exists
        if 'creation_tx_hash' in keys_address:
            if address_json["creation_tx_hash"] != None:
                contract.deployment_tx = address_json["creation_tx_hash"]

        # if creation_tx_hash exists, then call blockscout tx api endpoint for deployment date
        if contract.deployment_tx != None:
            try:
                tx_json = self.fetch_blockscout_data(self.load_params["aliases_blockscout_url"] + "transactions/", contract.deployment_tx)
                if 'timestamp' in tx_json.keys():
                    if tx_json["timestamp"] != None:
                        contract.deployment_date = tx_json["timestamp"]
                # if the transaction uses a method, we know is_factory_contract is True for the to_address of the deployment_tx!
                if 'method' in tx_json.keys():
                    if tx_json["method"] != None:
                        contract2 = AdapterBlockscout.Contract(
                            address = tx_json["to"]["hash"],
                            origin_key = contract.origin_key,
                            is_contract = True,
                            is_eoa = False,
                            is_factory_contract = True
                        )
                        contracts.append(contract2)
            except:
                print(f"Transaction not found on Blockscout for {contract.deployment_tx} on {contract.origin_key}")
        
        # map deployer_address if key exists
        if 'creator_address_hash' in keys_address:
            if address_json["creator_address_hash"] != None:
                contract.deployer_address = address_json["creator_address_hash"]

        # if is_verified is true, then call blockscout contract api endpoint for sourcify link
        if 'is_verified' in keys_address:
            if address_json["is_verified"] is True:
                try:
                    contract_json = self.fetch_blockscout_data(self.load_params["aliases_blockscout_url"] + "smart-contracts/", contract.address)
                except:
                    print(f"Contract not found on Blockscout for {contract.address} on {contract.origin_key}")
                if 'sourcify_repo_url' in contract_json.keys():
                    contract.source_code_verified = contract_json["sourcify_repo_url"]

        contracts.append(contract)

        return contracts

    def process_contract(self, contract: Contract) -> Contract:
        api_url = self.load_params["aliases_blockscout_url"]
        if not api_url:
            print(f"Unknown origin key for contract {contract.address}: {contract.origin_key}")
            return [contract]
        try:
            blockscout_json_address = self.fetch_blockscout_data(api_url + "addresses/", contract.address)
            #print(f"Processing address {contract.address} from {contract.origin_key}")
            #print(blockscout_json_address)
            return self.map_blockscout_to_open_labels(contract, blockscout_json_address)
        except (ValueError, TimeoutError) as e:
            print(f"Error processing address {contract.address}: {str(e)}")
            raise  # Re-raise the exception to be caught in process_contracts
        except Exception as e:
            print(f"Error processing address {contract.address}: {str(e)}")

        return [contract]

    def print_stats(self, stats: ProcessingStats):
        print("\n" + "="*40)
        print("Blockscout Statistics Summary".center(40))
        print("="*40)
        for attr in stats.__dict__:
            print(f"{attr.replace('_', ' ').capitalize():<30} : {getattr(stats, attr)}")
        print("="*40)

    def process_contracts(self, contracts: List[Contract]) -> List[Contract]:
        print(f"Starting to process {len(contracts)} {contracts[0].origin_key} contracts from Blockscout")
        results = []
        stats = AdapterBlockscout.ProcessingStats(total_contracts_input=len(contracts))
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for contract in contracts:
                future = executor.submit(self.process_contract, contract)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    processed_contracts = future.result()
                    results.extend(processed_contracts)
                    # keep track of stats
                    for c in processed_contracts:
                        stats.count_contracts += 1
                        if c.contract_name:
                            stats.count_contract_name += 1
                        if c.deployment_tx:
                            stats.count_deployment_tx += 1
                        if c.deployer_address:
                            stats.count_deployer_address += 1
                        if c.deployment_date:
                            stats.count_deployment_date += 1
                        if c.source_code_verified:
                            stats.count_source_code_verified += 1
                        if c.is_proxy:
                            stats.count_is_proxy += 1
                        if c.is_contract:
                            stats.count_is_contract += 1
                        if c.is_eoa:
                            stats.count_is_eoa += 1
                        if c.erc20_name:
                            stats.count_erc20_name += 1
                        if c.erc20_symbol:
                            stats.count_erc20_symbol += 1
                        if c.erc20_decimals:
                            stats.count_erc20_decimals += 1
                        if c.erc721_name:
                            stats.count_erc721_name += 1
                        if c.erc721_symbol:
                            stats.count_erc721_symbol += 1
                        if c.erc1155_name:
                            stats.count_erc1155_name += 1
                        if c.erc1155_symbol:
                            stats.count_erc1155_symbol += 1
                        if c.is_factory_contract:
                            stats.count_is_factory_contract += 1
                        # occasionally print progress
                        if stats.count_contracts % 200 == 0:
                            print(f"Processed {stats.count_contracts - stats.count_is_factory_contract}/{len(contracts)}...")
                except (ValueError, TimeoutError) as e:
                    print(f"Error occurred: {str(e)}")
                    print("Stopping the scraping process due to API issues.")
                    # Cancel all pending futures
                    for f in futures:
                        f.cancel()
                    # Wait for all futures to complete or be cancelled
                    wait(futures)
                    break  # Exit the loop
                except Exception as e:
                    print(f"Error processing contract: {str(e)}")

        # print stats
        stats.elapsed_time = f"{time.time() - start_time:.2f} seconds"
        self.print_stats(stats)

        return results

