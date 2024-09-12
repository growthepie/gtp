import json
import os
import pickle
from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List, Dict, Any
import numpy as np

# Use absolute paths
base_dir = os.path.abspath("src/metadata/chains")
abi_dir = os.path.abspath("src/metadata/token_abis")

# Ensure directories exist
os.makedirs(base_dir, exist_ok=True)
os.makedirs(abi_dir, exist_ok=True)

# Utility function to replace NaN with None
def replace_nan_with_none(data):
    if isinstance(data, dict):
        return {k: replace_nan_with_none(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_nan_with_none(v) for v in data]
    elif isinstance(data, float) and (np.isnan(data) or data is None):
        return None
    else:
        return data

# Utility function to clean keys by removing trailing/leading spaces
def clean_keys(data):
    if isinstance(data, dict):
        return {k.strip(): clean_keys(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_keys(v) for v in data]
    else:
        return data

# Define the Pydantic Model
class MainConfig(BaseModel):
    origin_key: str
    chain_type: str
    l2beat_stage: Optional[str]
    caip2: Optional[str]
    evm_chain_id: Optional[float]
    name: str
    name_short: str
    bucket: str
    block_explorers: Optional[dict]
    colors: dict
    logo: Optional[dict]
    ecosystem: list = Field(alias="ecosystem_old")

    ## API
    api_in_main: bool = Field(alias="api_in_api_main", default=False)
    api_in_fees: Optional[bool] = Field(alias="api_in_api_fees")
    api_in_economics: Optional[bool] = Field(alias="api_in_api_economics")
    api_in_labels: Optional[bool] = Field(alias="api_in_api_labels")
    api_deployment_flag: Optional[str] = Field(alias="api_api_deployment_flag")
    api_exclude_metrics: Optional[List[str]] = Field(alias="api_api_exclude_metrics")

    ## ALIASES
    aliases_l2beat: Optional[str] = Field(alias="aliases_l2beat")
    aliases_coingecko: Optional[str] = Field(alias="aliases_coingecko")
    aliases_rhino: Optional[str] = Field(alias="aliases_rhino")

    ## METADATA
    metadata_description: Optional[str] = Field(alias="metadata_description")
    metadata_symbol: Optional[str] = Field(alias="metadata_symbol")
    metadata_launch_date: Optional[str] = Field(alias="metadata_launch_date")
    metadata_da_layer: Optional[str] = Field(alias="metadata_da_layer")
    metadata_technology: Optional[str] = Field(alias="metadata_technology")
    metadata_purpose: Optional[str] = Field(alias="metadata_purpose")
    metadata_stack: Optional[Dict] = Field(alias="metadata_stack")
    metadata_raas: Optional[str] = Field(alias="metadata_raas")

    ## SOCIALS
    socials_website: Optional[HttpUrl] = Field(alias="socials_website", default=None)
    socials_twitter: Optional[HttpUrl] = Field(alias="socials_twitter", default=None)

    ## RUNS
    runs_aggregate_blockspace: Optional[bool] = Field(alias="runs_aggregate_blockspace", default=False)
    runs_aggregate_addresses: Optional[bool] = Field(alias="runs_aggregate_addresses", default=False)
    runs_contract_metadata: Optional[bool] = Field(alias="runs_contract_metadata", default=False)

    ## RPC CONFIG
    backfiller_on: Optional[bool] = Field(alias="backfiller_backfiller_on")
    backfiller_batch_size: Optional[int] = Field(default=20, alias="backfiller_batch_size")

    ## CROSS CHECK
    cross_check_url: Optional[HttpUrl] = Field(alias="cross_check_url")
    cross_check_type: Optional[str] = Field(alias="cross_check_type")

    ## CIRCULATING SUPPLY
    cs_token_address: Optional[str] = Field(alias="circulating_supply_token_address")
    cs_token_abi: Optional[Any] = Field(alias="circulating_supply_token_abi", default=None)
    cs_token_abi_file: Optional[str] = Field(alias="circulating_supply_token_abi_file", default=None)
    cs_deployment_date: Optional[str] = Field(alias="circulating_supply_token_deployment_date")
    cs_deployment_origin_key: Optional[str] = Field(alias="circulating_supply_token_deployment_origin_key")
    cs_supply_function: Optional[str] = Field(alias="circulating_supply_token_circulating_supply_function")

def save_abi(chain_data, file_name):
    # Check if circulating_supply_token_abi exists directly in the chain data
    if 'circulating_supply_token_abi' in chain_data:
        abi_data = chain_data['circulating_supply_token_abi']
        
        if abi_data:
            abi_file = os.path.join(abi_dir, f"{file_name}_abi.json")
            
            with open(abi_file, "w") as f:
                json.dump(abi_data, f, indent=4)
            
            # Store the ABI file path in the Pydantic model (so that it can be linked later)
            chain_data['circulating_supply_token_abi_file'] = abi_file
            
            # Remove the inline ABI since it's stored separately
            del chain_data['circulating_supply_token_abi']
        else:
            print(f"ABI for {file_name} is empty or invalid")
    else:
        print(f"'circulating_supply_token_abi' not found for {file_name}")

# Function to save main chain config as Pydantic objects
def save_main_config(db_connector):
    main_conf_dict = db_connector.get_main_config_dict()
    
    for chain_data in main_conf_dict:
        file_name = chain_data['origin_key']

        # Replace NaN values with None
        chain_data = replace_nan_with_none(chain_data)

        # Save ABI separately if exists
        save_abi(chain_data, file_name)

        pydantic_metadata_file = os.path.join(base_dir, f"{file_name}.pydantic")
        
        main_config_obj = MainConfig(**chain_data)
        
        with open(pydantic_metadata_file, "wb") as f:
            pickle.dump(main_config_obj, f)
        print(f"Saved Pydantic config for {file_name} at {pydantic_metadata_file}")
            
# Function to load metadata as Pydantic objects, safely handling missing files and cleaning keys
def get_main_config():
    config = []

    for file_name in os.listdir(base_dir):
        if file_name.endswith('.pydantic'):
            pydantic_file_path = os.path.join(base_dir, file_name)
            
            with open(pydantic_file_path, "rb") as f:
                main_config_obj = pickle.load(f)
                
                if main_config_obj.cs_token_abi is None and main_config_obj.cs_token_abi_file:
                    abi_file = main_config_obj.cs_token_abi_file
                    
                    if abi_file and os.path.exists(abi_file):
                        try:
                            with open(abi_file, 'r') as abi_f:
                                abi_data = json.load(abi_f)
                                # Attach the ABI back to the main config object
                                main_config_obj.cs_token_abi = abi_data
                        except FileNotFoundError:
                            print(f"ABI file {abi_file} not found.")
                
                config.append(main_config_obj)
    
    return config

# Function to get all L2 config by loading the metadata and appending predefined entries
def get_all_l2_config():
    main_config = get_main_config()
    
    # Add 'All L2s' config manually
    all_l2_config = main_config + [MainConfig(
        origin_key='all_l2s',
        chain_type='-', 
        name='All L2s', 
        name_short='-', 
        bucket='-', 
        colors={"light": ["#FFDF27", "#FE5468"], "dark": ["#FFDF27", "#FE5468"], "darkTextOnBackground": False}, 
        ecosystem_old=["op-stack", "op-super", "all-chains"], 
        api_in_api_main=True, 
        api_api_deployment_flag='PROD', 
        api_api_exclude_metrics=[]
    )]
    
    return all_l2_config

# Function to get multi-chain config, adding predefined entries to the existing L2 config
def get_multi_config():
    all_l2_config = get_all_l2_config()
    
    # Add 'Multiple L2s' config manually
    multi_config = all_l2_config + [MainConfig(
        origin_key='multiple',
        chain_type='all-L2s', 
        name='Multiple L2s', 
        name_short='-', 
        colors={"light": ["#cdd8d3", "#cdd8d3"], "dark": ["#cdd8d3", "#cdd8d3"], "darkTextOnBackground": False}, 
        bucket='-', 
        ecosystem_old=[], 
        api_in_api_main=True, 
        api_api_deployment_flag='PROD', 
        api_api_exclude_metrics=[]
    )]
    
    return multi_config