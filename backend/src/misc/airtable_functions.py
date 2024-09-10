### AIRTABLE
import pandas as pd

"""#initialize Airtable instance
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')"""

#-#-# GENERAL AIRTABLE FUNCTIONS #-#-#

# batch upload df to a table
def push_to_airtable(table, df):

    # convert rows to dictionary
    rows = df.to_dict(orient='records')

    # convert all timestamp columns to string
    for col in df.dtypes[df.dtypes == 'datetime64[ns]'].index:
        for row in rows:
            if pd.notnull(row[col]):
                row[col] = row[col].strftime('%Y-%m-%d %H:%M:%S')

    # convert NaT, nan to None for all columns
    for col in df.dtypes.index:
        for row in rows:
            if pd.isnull(row[col]):
                row[col] = None

    # push contracts to Airtable
    table.batch_create(rows)

    print(f"Pushed {len(rows)} records to Airtable.")


# delete all records from airtable
def clear_all_airtable(table):

    # get all ids (= rows) where the temp_owner_project field is not set (because we don't want to delete these contracts since the labelling is not finished for these)
    ids = [i['id'] for i in table.all() if 'temp_owner_project' not in i['fields'].keys()]

    # api can only handle batches of 10
    for i in range(0, len(ids), 10):
        batch_ids = ids[i:i+10]
        table.batch_delete(batch_ids)

    print(f"Deleted {len(ids)} records from Airtable.")


# read all rows from airtable into a df
def read_airtable(table):

    j = table.all()
    df = pd.DataFrame([{**r['fields'], 'id': r['id']} for r in j])
    
    return df   



#-#-# SPECIFIC AIRTABLE FUNCTIONS #-#-#

# get labelled items from airtable as a df
def read_all_labeled_contracts_airtable(api, AIRTABLE_BASE_ID, table):

    # get all records from airtable
    df = read_airtable(table)

    # check if anything new was labelled: ONLY ACCOUNTS FOR LABELLED STUFF IF ALSO 'labelling_type' COLUMN WAS SET!
    required_columns = ['labelling_type']
    if not any(col in df.columns for col in required_columns):
        print('no new labelled contracts found in airtable.')
        return

    # show only contracts that have been labeled
    required_columns = [col for col in required_columns if col in df.columns]
    df = df.dropna(subset=required_columns, how='all')

    # add all columns if they are missing, as api doesn't return empty columns
    if 'contract_name' not in df.columns:
        df['contract_name'] = ''
    if 'owner_project' not in df.columns:
        df['owner_project'] = None
    if 'usage_category' not in df.columns:
        df['usage_category'] = None
    if 'labelling_type' not in df.columns:
        df['labelling_type'] = ''
    if 'internal_description' not in df.columns:
        df['internal_description'] = None

    # drop not needded columns and clean df
    df = df[['address', 'origin_key', 'contract_name', 'owner_project', 'usage_category', 'labelling_type', 'internal_description']]
    df.rename(columns={'contract_name': 'name' , 'labelling_type' : 'source'}, inplace=True)

    # owner_project and usage_category are lists with 1 element, so we extract the element at index 0
    df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: x[0])
    df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: x[0])

    # convert column ids to text for owner_project & usage_category columns (e.g. recgawzTCg3ALuSR2 -> uniswap)
    if len(df[df["owner_project"].notna()]) > 0:
        df_owner_projects = read_airtable(api.table(AIRTABLE_BASE_ID, 'OSS Projects'))
        df_owner_projects = df_owner_projects[['id', 'Name']]
        df_owner_projects.set_index('id', inplace=True)
        df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: df_owner_projects.loc[x]['Name'])
    if len(df[df["usage_category"].notna()]) > 0:
        df_usage_categories = read_airtable(api.table(AIRTABLE_BASE_ID, 'Usage Categories'))
        df_usage_categories = df_usage_categories[['id', 'Category']]
        df_usage_categories.set_index('id', inplace=True)
        df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: df_usage_categories.loc[x]['Category'])

    # source is a dict with a name key, so we extract the first word
    df['source'] = df['source'].replace('', float('nan')) # replace empty strings with nan
    df['source'] = df[df['source'].notnull()]['source'].apply(lambda x: x['name'].split()[0])

    # convert address to bytes
    df['address'] = df['address'].apply(lambda x: x.replace('0x', '\\x'))

    return df
