import requests
import time
import sys
import json
import pandas as pd
import unicodedata
from datetime import datetime, timedelta
import boto3
import os
import eth_utils
import random
import numpy as np
from openai import OpenAI

import dotenv
dotenv.load_dotenv()

## API interaction functions
def api_get_call(url, sleeper=0.5, retries=15, header=None, _remove_control_characters=False, as_json=True, proxy=None):
    retry_counter = 0
    interupt = False

    while True:
        try:
            response = requests.request("GET", url, headers=header, proxies=proxy)
            if response.status_code == 200:
                break
            elif response.status_code == 400:
                print(f"400 error, Bad Request with: {url} and response: {response.text}") 
                return False
            elif response.status_code == 404:
                print(f"404 error, Not Found with: {url} and response: {response.text}")
                return False
            else:
                retry_counter += 1
                if retry_counter <= retries:
                    waiting_time = retry_counter ** 2
                    print(f"-- Code: {response.status_code} -- sleep for {str(waiting_time)}s then retry API call #{str(retry_counter)} with: {url}")
                    print(response.reason)
                    for i in range(1, waiting_time):
                        time.sleep(1)
                else:
                    print("retrying failed more than " + str(retries) + " times - Canceling")
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
        print("Execution ended successfully in api_post_call")
        sys.exit()
    else:
        if _remove_control_characters == True:
            return json.loads(remove_control_characters(response.text))
        else:
            return json.loads(response.text)

def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

## Adapter preparation functions
## this function checks if origin_keys exist in the projects:AdapterMapping and returns an error if one is missing
def check_projects_to_load(projects, origin_keys):
        if origin_keys is not None:
            for ok in origin_keys:
                ok_existing = False
                for p in projects:
                    if p.origin_key == ok:
                        ok_existing = True
                        break
                if ok_existing == False:
                    print(f'origin_key {ok} does not exist in AdapterMapping OR query doesnt exist.')
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

## this function returns a dataframe for the fact_kpis table with date column filled out
def get_df_kpis_with_dates(days):
    df = pd.DataFrame(columns=['date', 'metric_key', 'origin_key', 'value'])
    df['date'] = pd.date_range(end=datetime.today().date()-timedelta(days=1), periods=days).tolist()
    return df

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
        print(f"...no entry detected in tbl_kpis_daily for metric_key: {metric_key} and origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 10 #add 5 just for precaution (in case some data was missing etc.)
        print(f"...last entry in tbl_kpis_daily detected for metric_key: {metric_key} and origin_key: {origin_key} is on {last_date}. Set days to {days}.")
    return (days) 

def get_missing_days_blockspace(db_connector, origin_key):
    last_date = db_connector.get_blockspace_max_date(origin_key)
    if last_date == None:
        days = 9999
        print(f"...no blockspace entry detected for origin_key: {origin_key}. Set days to {days}.")
    else:
        delta = datetime.today().date() - last_date
        days = delta.days + 3 #add 3 just for precaution (in case some data was missing etc.)
        print(f"...last blockspace entry for origin_key: {origin_key} is on {last_date}. Set days to {days}.")
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

## convert df address columns to checksummed addresses
def db_addresses_to_checksummed_addresses(df, address_cols):
    for col in address_cols:
        df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(bytes(x)))
    return df

def string_addresses_to_checksummed_addresses(df, address_cols):
    for col in address_cols:
        ##df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(str(x)))
        ## this is a workaround for the above line, as it throws an error when the address is not a string
        df[col] = df[col].apply(lambda x: eth_utils.to_checksum_address(str(x)) if x and isinstance(x, str) else x)

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

## Discord functions
def send_discord_message(message, webhook_url=None):
    data = {"content": message}
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_ALERTS')
    response = requests.post(webhook_url, json=data)

    # Check the response status code
    if response.status_code == 204:
        print("Message sent successfully")
    else:
        print(f"Error sending message: {response.text}")

## Binance functions
def date_string_to_unix(date_string: str) -> int:
    dt = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    timestamp = int(dt.timestamp() * 1000)
    return timestamp

def get_binance_ohlc(symbol:str, start_time, end_time, interval='daily'):
    # Set API endpoint and parameters
    endpoint = 'https://api.binance.com/api/v3/klines'
    start = date_string_to_unix(start_time)
    end = date_string_to_unix(end_time)

    if interval == 'daily':
        interval_api = '1d'
        datapoints = (end - start) / (1000 * 60 * 60 * 24)
        second_calc = (1000 * 60 * 60 * 24 * 999)
    elif interval == 'hourly':
        interval_api = '1h'
        datapoints = (end - start) / (1000 * 60 * 60 * 24) * 24
        second_calc = (1000 * 60 * 60 * 24 * (999/24))
    
    dfMain = pd.DataFrame()
    while datapoints > 0:
        print(f'{int(datapoints)} datapoints left and current start time is {int(start)}')
        if start + second_calc > end:
            end_time = end
        else:
            end_time = start + second_calc

        params = {
            'symbol': symbol.upper()+'USDT',
            'interval': interval_api,
            'limit': 1000,
            'startTime': int(start),
            'endTime': int(end_time)
        }

        # Make GET request to API
        response = requests.get(endpoint, params=params)
        data = response.json()
        df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        dfMain = pd.concat([dfMain, df])

        start = start + second_calc
        
        datapoints -= 1000
        time.sleep(0.5)

    dfMain['Date'] = pd.to_datetime(dfMain['time'], unit='ms')
    dfMain.set_index('Date', inplace=True)
    dfMain = dfMain[['open', 'high', 'low', 'close', 'volume']]
    dfMain.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
    dfMain = dfMain.astype(float)
    return dfMain

## take binance ohlc input and output price_usd in correct db format
def prep_binance_ohlc(df, granularity, origin_key):
    df.reset_index(inplace=True)
    df = df.drop_duplicates(subset='Date', keep='first')

    df = df[['Date', 'Close']]
    df['metric_key'] = 'price_usd'

    df['origin_key'] = origin_key
    df['granularity'] = granularity

    ## rename columns Date to timestamp and Close to value
    df.rename(columns={'Date': 'timestamp', 'Close': 'value'}, inplace=True)

    df.set_index(['timestamp', 'metric_key', 'origin_key', 'granularity'], inplace=True)
    return df

## JSON helper functions
def replace_nan_with_none(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(elem) for elem in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None
    else:
        return obj

def count_nans_and_log_paths(obj, path=''):
    counter = 0
    paths = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            count, new_paths = count_nans_and_log_paths(v, new_path)
            counter += count
            paths.extend(new_paths)
    elif isinstance(obj, list):
        for i, elem in enumerate(obj):
            new_path = f"{path}[{i}]"
            count, new_paths = count_nans_and_log_paths(elem, new_path)
            counter += count
            paths.extend(new_paths)
    elif isinstance(obj, float) and np.isnan(obj):
        counter += 1
        paths.append(path)
    return counter, paths

def fix_dict_nan(test_dict, dict_name, send_notification=True):
    nan_count, nan_paths = count_nans_and_log_paths(test_dict)
    discord_webhook = os.getenv('DISCORD_TX_CHECKER')

    if nan_count > 0:
        nan_paths_str = '\n'.join(nan_paths)
        if send_notification:
            msg = f"Found {nan_count} NaNs in {dict_name} for the following paths:\n\n{nan_paths_str[:500]}..."
            send_discord_message(msg , discord_webhook)
        test_dict = replace_nan_with_none(test_dict)
        print(f"..WARNING: replaced {nan_count} NaNs in {dict_name}")
    else:
        print(f"..no NaNs found in {dict_name}")
    
    return test_dict


## S3 functions
def empty_cloudfront_cache(distribution_id, path):
        cf = boto3.client('cloudfront')
        print("Creating invalidation for path: " + path)
        time.sleep(random.randint(1, 3))
        res = cf.create_invalidation(
            DistributionId=distribution_id,
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
        #print("Invalidation created successfully with Id: " + invalidation_id)
        return invalidation_id

def upload_file_to_cf_s3(bucket, path_name, local_path, cf_distribution_id):
    # Initialize S3 client
    s3 = boto3.client("s3")
    # Upload the file to S3
    s3.upload_file(local_path, bucket, path_name)

    print(f'..uploaded to {path_name}')
    empty_cloudfront_cache(cf_distribution_id, f'/{path_name}')


def upload_json_to_cf_s3(bucket, path_name, details_dict, cf_distribution_id, invalidate=True):
    # Convert Dictionary to JSON String
    details_json = json.dumps(details_dict)

    # Upload JSON String to an S3 Object
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket, 
        Key=f'{path_name}.json',
        Body=details_json,
        ContentType='application/json'
    )

    print(f'..uploaded to {path_name}')
    if invalidate:
        empty_cloudfront_cache(cf_distribution_id, f'/{path_name}.json')

def remove_file_from_s3(bucket, path_name):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    obj = s3.Object(bucket, path_name)
    obj.delete()
    print(f"Deleted {path_name}")

def get_files_df_from_s3(bucket_name, prefix):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        files.append([obj.key, obj.last_modified])

    df = pd.DataFrame(files, columns=['key', 'last_modified'])
    df.sort_values(by='last_modified', ascending=False, inplace=True)
    return df
    
def upload_png_to_cf_s3(bucket, s3_path, local_path, cf_distribution_id):
    print(f'...uploading png from {local_path} to {s3_path} in bucket {bucket}')
    # Upload JSON String to an S3 Object
    s3 = boto3.client('s3')
    with open(local_path, 'rb') as image:
        s3.put_object(
            Bucket=bucket, 
            Key=f'{s3_path}.png',
            Body=image.read(),
            ContentType='image/png'
        )

    print(f'..uploaded to {s3_path}')
    empty_cloudfront_cache(cf_distribution_id, f'/{s3_path}.png')

def upload_parquet_to_cf_s3(bucket, path_name, df, cf_distribution_id):
    s3_url = f"s3://{bucket}/{path_name}.parquet"
    df.to_parquet(s3_url)

    print(f'..uploaded to {path_name}')
    empty_cloudfront_cache(cf_distribution_id, f'/{path_name}.parquet')

def upload_polars_df_to_s3(df, file_name, bucket, key):
    df.write_parquet(file_name)

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, key)
        os.remove(file_name)
    except FileNotFoundError:
        print(f'The file {file_name} was not found')

## This function uploads a dataframe to S3 longterm bucket as parquet file
def dataframe_to_s3(path_name, df):
    s3_url = f"s3://{os.getenv('S3_LONG_TERM_BUCKET')}/{path_name}.parquet"
    df.to_parquet(s3_url)

    print(f'...uploaded to S3 longterm in {path_name}')

# prompt chatgpt, requires openai library
def prompt_chatgpt(prompt, api_key, model="gpt-3.5-turbo"):
    client = OpenAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"An error occurred while prompting chatgpt: {str(e)}"
    
# convert yml file object (dict) into a df
def convert_economics_mapping_into_df(data): # turns yml object of economics_mapping.yml into a dataframe
    table = [
        [
            L2,
            layers.get('name'), 
            settlement_layer, 
            f.get('from_address'), 
            f.get('to_address'), 
            f.get('method'), 
            f.get('namespace') if settlement_layer == 'celestia' else None
        ]
        for L2, layers in data.items()
        for settlement_layer, filters in layers.items() if isinstance(filters, list)
        for f in filters
    ]
    df = pd.DataFrame(table, columns=['origin_key', 'name', 'da_layer', 'from_address', 'to_address', 'method', 'namespace'])
    return df