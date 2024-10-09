from pydantic import BaseModel, HttpUrl, Field, validator
from typing import Optional, List, Dict, Any
from src.db_connector import DbConnector
import zipfile
import io
import json
import requests

class MainConfig(BaseModel):
    origin_key: str
    chain_type: str
    l2beat_stage: str = "NA" ## default NA
    caip2: Optional[str]
    evm_chain_id: Optional[int]
    name: str
    name_short: str
    bucket: str
    block_explorers: Optional[dict]
    colors: dict
    logo: dict = {
        'body': "<svg width=\"15\" height=\"15\" viewBox=\"0 0 15 15\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\"><circle cx=\"7.5\" cy=\"7.5\" r=\"7.5\" fill=\"currentColor\"/></svg>",
        'width': 15,
        'height': 15
    }
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
    aliases_l2beat_slug: Optional[str] = Field(alias="aliases_l2beat_slug")
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
    socials_website: Optional[HttpUrl] = Field(alias="socials_website")
    socials_twitter: Optional[HttpUrl] = Field(alias="socials_twitter ")

    ## RUNS
    runs_aggregate_blockspace: Optional[bool] = Field(alias="runs_aggregate_blockspace")
    runs_aggregate_addresses: Optional[bool] = Field(alias="runs_aggregate_addresses ")
    runs_contract_metadata: Optional[bool] = Field(alias="runs_contract_metadata")

    ## RPC CONFIG
    backfiller_on: Optional[bool] = Field(alias="backfiller_backfiller_on")
    backfiller_batch_size: Optional[int] = Field(20, alias="backfiller_batch_size")

    ## CROSS CHECK
    cross_check_url: Optional[HttpUrl] = Field(alias="cross_check_url")
    cross_check_type: Optional[str] = Field(alias="cross_check_type")

    ## CIRCULATING SUPPLY
    cs_token_address: Optional[str] = Field(alias="circulating_supply_token_address")
    cs_token_abi: Optional[Any] = Field(alias="circulating_supply_token_abi")
    cs_deployment_date: Optional[str] = Field(alias="circulating_supply_token_deployment_date")
    cs_deployment_origin_key: Optional[str] = Field(alias="circulating_supply_token_deployment_origin_key")
    cs_supply_function: Optional[str] = Field(alias="circulating_supply_token_supply_function")

    ## VALIDATOR to set default values if field exists with None in dictionary
    @validator('logo', 'l2beat_stage', pre=True, always=True)
    def set_default_if_none(cls, v, field):
        if v is None:
            return field.default
        return v

# def get_main_config(db_connector:DbConnector):
#     main_conf_dict = db_connector.get_main_config_dict()
#     return [MainConfig(**data) for data in main_conf_dict]

def get_main_config_dict(db_connector:DbConnector):
    # Get the repository
    repo_url = "https://github.com/growthepie/chain-registry/tree/main/"
    _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')
    path = '/'.join(path)

    # Download oss-directory as ZIP file
    zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
    response = requests.get(zip_url)
    zip_content = io.BytesIO(response.content)

    main_config_dict = []

    with zipfile.ZipFile(zip_content) as zip_ref:
        nameslist = zip_ref.namelist()
        ## only exact folders from nameslist
        chains = [name.split('/')[1] for name in nameslist if name.endswith('/')]
        chains = [chain for chain in chains if chain]

        for chain in chains:      
            chain_data = {}
            path = f"chain-registry-main/{chain}"

            main_path = f"{path}/main.json"
            logo_path = f"{path}/logo.json"
            token_abi_path = f"{path}/token_abi.json"

            if main_path in nameslist:
                with zip_ref.open(main_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    chain_data = content
                    
            if logo_path in nameslist:
                with zip_ref.open(logo_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    chain_data["logo"] = content
                                
            if token_abi_path in nameslist:
                with zip_ref.open(token_abi_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    chain_data["circulating_supply_token_abi"] = content

            ##chain_data['l2beat_stage'] = db_connector.get_stage(chain)
            main_config_dict.append(chain_data)

    return main_config_dict

def get_main_config(db_connector:DbConnector, main_config_dict=None):
    if not main_config_dict:
        main_config_dict = get_main_config_dict(db_connector)

    main_config = [MainConfig(**data) for data in main_config_dict]

    ## add dynamic l2beat stage info
    stages = db_connector.get_stages_dict()
    ## iterate over stages and update the new_config with the stage
    for stage in stages:
        chain = [chain for chain in main_config if chain.origin_key == stage]
        if chain:
            chain[0].l2beat_stage = stages[stage]

    return main_config

def get_all_l2_config(db_connector:DbConnector):
    main_config = get_main_config(db_connector)
    all_l2_config = main_config + [MainConfig(origin_key='all_l2s', chain_type = '-', name='All L2s', name_short='-', bucket='-', colors={"light":["#FFDF27","#FE5468"],"dark":["#FFDF27","#FE5468"],"darkTextOnBackground": False}, ecosystem_old=["op-stack", "op-super", "all-chains"], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return all_l2_config

def get_multi_config(db_connector:DbConnector):
    all_l2_config = get_all_l2_config(db_connector)
    multi_config = all_l2_config + [MainConfig(origin_key='multiple', chain_type = 'all-L2s', name='Multiple L2s', name_short='-', colors = {"light":["#cdd8d3","#cdd8d3"],"dark":["#cdd8d3","#cdd8d3"],"darkTextOnBackground": False} , bucket='-', ecosystem_old=[], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return multi_config