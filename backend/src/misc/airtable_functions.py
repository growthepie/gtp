### AIRTABLE
import pandas as pd
import airtable
import os

#initialize Airtable instance
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
at = airtable.Airtable(AIRTABLE_BASE_ID, AIRTABLE_API_KEY)


def push_to_airtable(df):

    #add urls
    df['Github Search'] = df['address'].apply(lambda x: f'https://github.com/search?q={x}&type=code')
    urls_arbitrum = df['address'].loc[df['origin_key'] == 'arbitrum'].apply(lambda x: f'https://arbiscan.io/address/{x}')
    urls_optimism = df['address'].loc[df['origin_key'] == 'optimism'].apply(lambda x: f'https://optimistic.etherscan.io/address/{x}')
    urls_base = df['address'].loc[df['origin_key'] == 'base'].apply(lambda x: f'https://basescan.org/address/{x}')
    urls_zksync_era = df['address'].loc[df['origin_key'] == 'zksync_era'].apply(lambda x: f'https://explorer.zksync.io/address/{x}')
    urls_polygon_zkevm = df['address'].loc[df['origin_key'] == 'polygon_zkevm'].apply(lambda x: f'https://zkevm.polygonscan.com/address/{x}')
    #urls_ = df['address'].loc[df['origin_key'] == ''].apply(lambda x: f'{x}')
    df['Blockexplorer'] = pd.concat([urls_arbitrum, urls_optimism, urls_base, urls_zksync_era, urls_polygon_zkevm])

    #convert df to dict of rows
    contracts = df.to_dict(orient='records')

    #push contracts to Airtable
    for c in contracts:
        at.create('Unlabeled Contracts', c)

    print(f"Pushed {len(contracts)} records to Airtable.")


def clear_all_airtable():
    print('Starting to delete all records from Airtable...')
    #get record IDs to delete
    c_ids = [c['id'] for c in at.get('Unlabeled Contracts')['records']]

    #delete records one by one
    for i in c_ids:
        at.delete('Unlabeled Contracts', i)

    print("All records have been deleted from the table.")


def read_all_airtable():

    #get table data and convert to df
    data = pd.DataFrame([c['fields'] for c in at.get('Unlabeled Contracts')['records']])

    if 'sub_category_key' not in data.columns:
        print('nothing labeled here...')
        return False

    #show only contracts that have been labeled
    data = data[data['sub_category_key'].notnull()]
    data = data[data['address'].notnull()]
    data = data[data['origin_key'].notnull()]

    #add all needed columns, as api doesn't return empty columns
    if 'contract_name' not in data.columns:
        data['contract_name'] = ''
    if 'project_name' not in data.columns:
        data['project_name'] = ''
    if 'labelling_type' not in data.columns:
        data['labelling_type'] = ''

    #drop not needded columns and clean data
    data = data[['address', 'origin_key', 'contract_name', 'project_name', 'sub_category_key', 'labelling_type']]
    data['labelling_type'] = data[data['labelling_type'].notnull()]['labelling_type'].apply(lambda x: x['name'].split()[0])

    #clean data for db upload
    data['address'] = data['address'].apply(lambda x: x.replace('0x', '\\x'))

    return data
