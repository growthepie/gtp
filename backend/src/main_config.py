from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List, Dict, Any

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


def get_main_config(db_connector):
    main_conf_dict = db_connector.get_main_config_dict()
    return [MainConfig(**data) for data in main_conf_dict]


def get_all_l2_config(db_connector):
    main_config = get_main_config(db_connector)
    all_l2_config = main_config + [MainConfig(origin_key='all_l2s', chain_type = '-', name='All L2s', name_short='-', bucket='-', colors={"light":["#FFDF27","#FE5468"],"dark":["#FFDF27","#FE5468"],"darkTextOnBackground": False}, ecosystem_old=["op-stack", "op-super", "all-chains"], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return all_l2_config

def get_multi_config(db_connector):
    all_l2_config = get_all_l2_config(db_connector)
    multi_config = all_l2_config + [MainConfig(origin_key='multiple', chain_type = 'all-L2s', name='Multiple L2s', name_short='-', colors = {"light":["#cdd8d3","#cdd8d3"],"dark":["#cdd8d3","#cdd8d3"],"darkTextOnBackground": False} , bucket='-', ecosystem_old=[], api_in_api_main=True, api_api_deployment_flag='PROD', api_api_exclude_metrics=[])] ## for multi-chain metrics
    return multi_config