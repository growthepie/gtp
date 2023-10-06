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
    urls_zora = df['address'].loc[df['origin_key'] == 'zora'].apply(lambda x: f'https://explorer.zora.energy/address/{x}')
    urls_gitcoin_pgn = df['address'].loc[df['origin_key'] == 'gitcoin_pgn'].apply(lambda x: f'https://explorer.publicgoods.network/address/{x}')
    #urls_ = df['address'].loc[df['origin_key'] == ''].apply(lambda x: f'{x}')
    df['Blockexplorer'] = pd.concat([urls_arbitrum, urls_optimism, urls_base, urls_zksync_era, urls_polygon_zkevm, urls_zora, urls_gitcoin_pgn])

    #convert df to dict of rows
    contracts = df.to_dict(orient='records')

    #push contracts to Airtable
    for c in contracts:
        at.create('Unlabeled Contracts', c)

    print(f"Pushed {len(contracts)} records to Airtable.")


def clear_all_airtable():

    print('Starting to delete all records from Airtable...')

    offset = None
    while True:
        j = at.get('Unlabeled Contracts', offset = offset)

        #delete records one by one
        for i in [c['id'] for c in j['records']]:
            at.delete('Unlabeled Contracts', i)
        
        if len(j) == 2:
            offset = j['offset']
        else:
            break

    print("All records have been deleted from the table.")


def read_all_airtable():

    #get complete airtable and create df
    df = pd.DataFrame()
    offset = None
    while True:
        j = at.get('Unlabeled Contracts', offset = offset)
        df = pd.concat([df, pd.DataFrame([c['fields'] for c in j['records']])], ignore_index = True)
        if len(j) == 2:
            offset = j['offset']
        else:
            break

    if 'sub_category_key' not in df.columns:
        print('no new labelled contracts found in the airtable.')
        return

    #show only contracts that have been labeled
    df = df[df['sub_category_key'].notnull()]
    df = df[df['address'].notnull()]
    df = df[df['origin_key'].notnull()]

    #add all needed columns, as api doesn't return empty columns
    if 'contract_name' not in df.columns:
        df['contract_name'] = ''
    if 'project_name' not in df.columns:
        df['project_name'] = ''
    if 'labelling_type' not in df.columns:
        df['labelling_type'] = ''

    #drop not needded columns and clean df
    df = df[['address', 'origin_key', 'contract_name', 'project_name', 'sub_category_key', 'labelling_type']]
    df['labelling_type'] = df[df['labelling_type'].notnull()]['labelling_type'].apply(lambda x: x['name'].split()[0])

    #clean df for db upload
    df['address'] = df['address'].apply(lambda x: x.replace('0x', '\\x'))

    return df
