from pydantic import BaseModel, HttpUrl, Field, field_validator, FieldValidationInfo
from typing import Optional
import zipfile
import io
import json
import requests
import pickle

class DAConfig(BaseModel):
    da_layer: str
    name: str
    name_short: str
    block_explorers: Optional[dict]

    colors: dict
    logo: dict = {
        'body': "<svg width=\"15\" height=\"15\" viewBox=\"0 0 15 15\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\"><circle cx=\"7.5\" cy=\"7.5\" r=\"7.5\" fill=\"currentColor\"/></svg>",
        'width': 15,
        'height': 15
    } 

    ## API
    incl_in_da_overview: bool = Field(alias="incl_in_da_overview", default=False)
   
    ## METADATA
    parameters: Optional[dict]

    ## SOCIALS
    socials_website: Optional[HttpUrl] = Field(alias="socials_website")
    socials_twitter: Optional[HttpUrl] = Field(alias="socials_twitter ")

    ## VALIDATOR to set default values if field exists with None in dictionary
    @field_validator('logo', mode='before')
    def set_default_if_none(cls, v, info: FieldValidationInfo):
        if v is None:
            return info.field_info.default
        return v

def get_da_config_dict():
    # Get the repository
    repo_url = "https://github.com/growthepie/gtp-dna/tree/main/"
    _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')

    # Download directory as ZIP file
    zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
    response = requests.get(zip_url)
    zip_content = io.BytesIO(response.content)

    da_config_dict = []

    with zipfile.ZipFile(zip_content) as zip_ref:
        root_path = 'gtp-dna-main/da_layers/'
        nameslist = zip_ref.namelist()
        nameslist = [name for name in nameslist if name.startswith(root_path)]

        ## only exact folders from nameslist
        da_layers = [name.split('/')[2] for name in nameslist if name.endswith('/')]
        da_layers = [da_layer for da_layer in da_layers if da_layer]

        for da_layer in da_layers:      
            da_layer_data = {}
            path = f"{root_path}{da_layer}"

            main_path = f"{path}/main.json"
            logo_path = f"{path}/logo.json"

            if main_path in nameslist:
                with zip_ref.open(main_path) as file:
                    content = file.read().decode('utf-8')
                    content = json.loads(content)
                    da_layer_data = content
                    
                if logo_path in nameslist:
                    with zip_ref.open(logo_path) as file:
                        content = file.read().decode('utf-8')
                        content = json.loads(content)
                        da_layer_data["logo"] = content

                da_config_dict.append(da_layer_data)

    return da_config_dict

def get_da_config(da_config_dict=None, source='s3'):
    if source == 's3':
        response = requests.get("https://api.growthepie.xyz/v1/da_conf.pkl")
        da_config = pickle.loads(response.content)
    elif source == 'github':
        if not da_config_dict:
            da_config_dict = get_da_config_dict()

        da_config = [DAConfig(**data) for data in da_config_dict]
    else:
        raise NotImplementedError(f"Source {source} not implemented")

    return da_config