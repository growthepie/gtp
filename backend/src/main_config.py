from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List, Dict   

class MainConfig(BaseModel):
    origin_key: str
    l2beat_stage: Optional[str]
    caip2: Optional[str]
    evm_chain_id: Optional[float]
    name: str
    name_short: str
    bucket: str
    block_explorers: Optional[dict]

    ## API
    api_in_main: bool = Field(alias="api.in_api_main", default=False)
    api_in_fees: Optional[bool] = Field(alias="api.in_api_fees")
    api_in_economics: Optional[bool] = Field(alias="api.in_api_economics")
    api_in_labels: Optional[bool] = Field(alias="api.in_api_labels")
    api_deployment_flag: Optional[str] = Field(alias="api.api_deployment_flag")
    api_exclude_metrics: Optional[List[str]] = Field(alias="api.api_exclude_metrics")

    ## ALIASES
    aliases_l2beat: Optional[str] = Field(alias="aliases.l2beat")
    aliases_coingecko: Optional[str] = Field(alias="aliases.coingecko")
    aliases_rhino: Optional[str] = Field(alias="aliases.rhino")

    ## METADATA
    metadata_description: Optional[str] = Field(alias="metadata.description")
    metadata_symbol: Optional[str] = Field(alias="metadata.symbol")
    metadata_launch_date: Optional[str] = Field(alias="metadata.launch_date")
    metadata_da_layer: Optional[str] = Field(alias="metadata.da_layer")
    metadata_technology: Optional[str] = Field(alias="metadata.technology")
    metadata_purpose: Optional[str] = Field(alias="metadata.purpose")
    metadata_stack: Optional[Dict] = Field(alias="metadata.stack")
    metadata_raas: Optional[str] = Field(alias="metadata.raas")

    ## SOCIALS
    socials_website: Optional[HttpUrl] = Field(alias="socials.website")
    socials_twitter: Optional[HttpUrl] = Field(alias="socials.twitter ")

    ## RUNS
    runs_aggregate_blockspace: Optional[bool] = Field(alias="runs.aggregate_blockspace")
    runs_aggregate_addresses: Optional[bool] = Field(alias="runs.aggregate_addresses ")
    runs_contract_metadata: Optional[bool] = Field(alias="runs.contract_metadata")

    ## BACKFILLER
    backfiller_on: Optional[bool] = Field(alias="backfiller.backfiller_on")
    backfiller_batch_size: Optional[float] = Field(alias="backfiller.batch_size")

    ## CROSS CHECK
    cross_check_url: Optional[HttpUrl] = Field(alias="cross_check.url")
    cross_check_type: Optional[str] = Field(alias="cross_check.type")

def get_main_config(db_connector):
    main_conf_dict = db_connector.get_main_config_dict()
    return [MainConfig(**data) for data in main_conf_dict]
