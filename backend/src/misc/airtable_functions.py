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

    # convert df to dict of rows
    rows = df.to_dict(orient='records')

    # push contracts to Airtable
    table.batch_create(rows)

    print(f"Pushed {len(rows)} records to Airtable.")


# delete all records from airtable
def clear_all_airtable(table):

    # get all ids (= rows)
    ids = [i['id'] for i in table.all()]

    # api can only handle batches of 10
    for i in range(0, len(ids), 10):
        batch_ids = ids[i:i+10]
        table.batch_delete(batch_ids)

    print(f"Deleted {len(ids)} records from Airtable.")


# read all rows from airtable into a df
def read_airtable(table):

    j = table.all()
    df = pd.DataFrame([r['fields'] for r in j])
    
    return df   



#-#-# SPECIFIC AIRTABLE FUNCTIONS #-#-#

# get labelled items from airtable as a df
def read_all_labeled_contracts_airtable(table):

    # get all records from airtable
    df = read_airtable(table)

    # check if anything was labelled
    required_columns = ['sub_category_key', 'address', 'origin_key']
    if not all(col in df.columns for col in required_columns):
        print('no new labelled contracts found in airtable.')
        return

    # show only contracts that have been labeled
    df = df.dropna(subset=['sub_category_key', 'address', 'origin_key'])

    # add all columns if they are missing, as api doesn't return empty columns
    if 'contract_name' not in df.columns:
        df['contract_name'] = ''
    if 'project_name' not in df.columns:
        df['project_name'] = ''
    if 'labelling_type' not in df.columns:
        df['labelling_type'] = ''

    # drop not needded columns and clean df
    df = df[['address', 'origin_key', 'contract_name', 'project_name', 'sub_category_key', 'labelling_type']]
    df['labelling_type'] = df[df['labelling_type'].notnull()]['labelling_type'].apply(lambda x: x['name'].split()[0])

    # convert address to bytes
    df['address'] = df['address'].apply(lambda x: x.replace('0x', '\\x'))

    return df
