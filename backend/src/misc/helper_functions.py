import requests
import time
import sys
import json
import pandas as pd
import unicodedata
from datetime import datetime
import boto3
import os

## API interaction functions
def api_get_call(url, sleeper=0.5, retries=15, header=None, _remove_control_characters=False, as_json=True):
    retry_counter = 0
    interupt = False

    while True:
        try:
            response = requests.request("GET", url, headers=header)
            if response.status_code == 200:
                break
            elif response.status_code == 400:
                print(f"400 error, Bad Request with: {url} and response: {response.text}") 
                return "400"
            else:
                retry_counter += 1
                if retry_counter <= retries:
                    waiting_time = retry_counter ** 2
                    print(f"-- Code: {response.status_code} -- sleep for {str(waiting_time)}s then retry API call #{str(retry_counter)} with: {url}")
                    print(response.reason)
                    for i in range(1, waiting_time):
                        time.sleep(1)
                else:
                    print("retrying failed more than " + str(retries) + " times - start over")
                    return False
        except KeyboardInterrupt:
            interupt = True
            break        
        except:
            print('request issue, will retry with: ' + url)
            time.sleep(retry_counter * sleeper)
    
    if interupt == True:
        print("Execution ended successfully in api_get_call")
        sys.exit()
    else:
        if _remove_control_characters == True:
            if as_json == True:
                return json.loads(remove_control_characters(response.text))
            else:
                return remove_control_characters(response.text)
        else:
            if as_json == True:
                return json.loads(response.text)
            else:
                return response.text

def api_post_call(url, payload, sleeper=0.5, retries=15, header=None, _remove_control_characters=False):
    retry_counter = 0
    interupt = False

    while True:
        try:
            response = requests.request("POST", url, data=payload, headers=header)
            if response.status_code == 200:
                break
            else:
                retry_counter += 1
                if retry_counter <= retries:
                    waiting_time = retry_counter ** 2
                    print(f"-- Code: {response.status_code} -- sleep for {str(waiting_time)}s then retry API call #{str(retry_counter)} with: {url}")
                    print(response.reason)
                    for i in range(1, waiting_time):
                        time.sleep(1)
                else:
                    print("retrying failed more than " + str(retries) + " times - start over")
                    return False
        except KeyboardInterrupt:
            interupt = True
            break        
        except:
            print('request issue, will retry with: ' + url)
            time.sleep(retry_counter * sleeper)
    
    if interupt == True:
        print("Execution ended successfully in api_get_call")
        sys.exit()
    else:
        if _remove_control_characters == True:
            return json.loads(remove_control_characters(response.text))
        else:
            return json.loads(response.text)

def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

## Adapter preparation functions
## this function checks if origin_keys exist in the projects:AdapterMapping or FlipsideQueries and returns an error if one is missing
def check_projects_to_load(projects, origin_keys):
        if origin_keys is not None:
            for ok in origin_keys:
                ok_existing = False
                for p in projects:
                    if p.origin_key == ok:
                        ok_existing = True
                        break
                if ok_existing == False:
                    print(f'origin_key {ok} does not exist in AdapterMapping.')
                    raise NotImplementedError

## this function checks if query_name exist in the zettablock_queries and returns an error if one is missing
def check_zb_queries_to_load(zb_queries, query_names):
        for qname in query_names:
            qname_existing = False
            for zb in zb_queries:
                if zb.query_name == qname:
                    qname_existing = True
                    break
            if qname_existing == False:
                print(f'query name {qname} does not exist in zettablock_queries.py')
                raise NotImplementedError

## this function filters the projects:AdapterMapping and only returns origin keys defined in origin_keys
def return_projects_to_load(projects, origin_keys):
        if origin_keys is not None:
            projects_to_load = [x for x in projects if x.origin_key in origin_keys]
        else:
            projects_to_load = projects
        return projects_to_load

## this function returns a dataframe for the fact_kpis table
def get_df_kpis():
    return pd.DataFrame(columns=['date', 'metric_key', 'origin_key', 'value'])

## this function upserts a dataframe to the fact_kpis and returns the number of upserted rows
def upsert_to_kpis(df, db_connector):
    tbl_name = 'fact_kpis'
    upserted = db_connector.upsert_table(tbl_name, df)
    return upserted, tbl_name

## this function returns the number of days between today and the last entry in fact_kpis
def get_missing_days_kpis(db_connector, metric_key, origin_key):
    last_date = db_connector.get_max_date(metric_key, origin_key)
    if last_date == None:
        days = 9999
        print(f"No entry detected in tbl_kpis_daily for metric_key: {metric_key} and origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 5 #add 5 just for precaution (in case some data was missing etc.)
        print(f"Last entry in tbl_kpis_daily detected for metric_key: {metric_key} and origin_key: {origin_key} is on {last_date}. Set days to {days}.")
    return (days) 

def get_missing_days_blockspace(db_connector, origin_key):
    last_date = db_connector.get_blockspace_max_date(origin_key)
    if last_date == None:
        days = 9999
        print(f"No entry detected in blockspace_fact_contract_level for origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 3 #add 5 just for precaution (in case some data was missing etc.)
        print(f"Last entry in blockspace_fact_contract_level detected for origin_key: {origin_key} is on {last_date}. Set days to {days}.")
    return (days) 

## prepare df for kpis_daily with input df having day and value columns
def prepare_df_kpis(df, metric_key, origin_key):
        df['day'] = df['day'].apply(pd.to_datetime)
        df['date'] = df['day'].dt.date
        df.drop(['day'], axis=1, inplace=True)
        df['metric_key'] = metric_key
        df['origin_key'] = origin_key
        # max_date = df['date'].max()
        # df.drop(df[df.date == max_date].index, inplace=True)
        today = datetime.today().strftime('%Y-%m-%d')
        df.drop(df[df.date == today].index, inplace=True, errors='ignore')
        df.value.fillna(0, inplace=True)
        return df

## Some simple Adapter print functions
def clean_params(params:dict):
    if 'api_key' in params:
        params['api_key'] = '***'
    if 'infura_api' in params:
        params['infura_api'] = '***'
    return params

def print_init(name:str, params:dict):
    params = clean_params(params)
    print(f"Adapter {name} initialized with {params}.")

def print_extract(name:str, params:dict, df_shape):
    params = clean_params(params)
    print(f'{name} extract done for {params}. DataFrame shape: {df_shape}')

def print_extract_raw(name:str, df_shape):
    print(f'Extract {name} RAW done. DataFrame shape: {df_shape}')

def print_load(name:str, upserted:int, tbl_name:str):
    print(f'Load {name} done - {upserted} rows upserted in {tbl_name}')

def print_load_raw(name:str, upserted:int, tbl_name:str):
    print(f'Load {name} RAW done - {upserted} rows upserted in {tbl_name}')

def print_orchestration_raw_start(name:str):
    print(f'Orchestration {name} RAW started.')

def print_orchestration_raw_end(name:str):
    print(f'Orchestration {name} RAW finished.')


## S3 functions
def empty_cloudfront_cache(distrubution_id, path):
        cf = boto3.client('cloudfront')
        # print("Creating invalidation for path: " + path)
        res = cf.create_invalidation(
            DistributionId=distrubution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': [
                        path
                    ]
                },
                'CallerReference': str(time.time()).replace(".", "")
            }
        )
        invalidation_id = res['Invalidation']['Id']
        time.sleep(2)
        #print("Invalidation created successfully with Id: " + invalidation_id)
        return invalidation_id

def upload_json_to_cf_s3(bucket, path_name, details_dict, cf_distribution_id):
    # Convert Dictionary to JSON String
    details_json = json.dumps(details_dict)

    # Upload JSON String to an S3 Object
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket, 
        Key=f'{path_name}.json',
        Body=details_json
    )

    print(f'... uploaded to {path_name}')
    empty_cloudfront_cache(cf_distribution_id, f'/{path_name}.json')


## This function uploads a dataframe to S3 longterm bucket as parquet file
def dataframe_to_s3(path_name, df):
    s3_url = f"s3://{os.getenv('S3_LONG_TERM_BUCKET')}/{path_name}.parquet"
    df.to_parquet(s3_url)

    print(f'...uploaded to S3 longterm in {path_name}')

