import requests
import pandas as pd
from adapter_utils import create_db_engine, load_environment
from pangres import upsert

def extract_token_info_from_endpoint(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

    data = response.json()

    extracted_info = []
    for token in data:
        token_id = token.get("tokenId")
        symbol = token.get("symbol")
        decimals = token.get("decimals")
        extracted_info.append({"token_id": token_id, "symbol": symbol, "decimals": decimals})

    return extracted_info

def insert_into_db(df, engine, table_name):
    # Set the DataFrame's index to your primary key
    df.set_index('token_id', inplace=True)
    df.index.name = 'token_id'
    
    # Insert data into database
    print(f"Inserting data into table {table_name}...")
    try:
        upsert(engine, df, table_name, if_row_exists='update')
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data into table {table_name}.")
        print(e)

def main():
    url = "https://api3.loopring.io/api/v3/exchange/tokens"
    token_info = extract_token_info_from_endpoint(url)
    df = pd.DataFrame(token_info)

    db_name, db_user, db_password, db_host, db_port = load_environment()
    engine = create_db_engine(db_user, db_password, db_host, db_port, db_name)

    insert_into_db(df, engine, "loopring_tokens")

if __name__ == "__main__":
    main()
