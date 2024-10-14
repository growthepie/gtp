import numpy as np
from web3 import Web3
from web3.middleware import geth_poa_middleware
from src.db_connector import DbConnector
from backend.src.adapters.rpc_funcs.utils import prep_dataframe_new
from src.adapters.rpc_funcs.utils import (
    MaxWaitTimeExceededException,
    fetch_data_for_range,
    prep_dataframe_linea,
    prep_dataframe_scroll,
    prep_dataframe_arbitrum,
    prep_dataframe_polygon_zkevm,
    prep_dataframe_zksync_era,
    prep_dataframe_eth,
    prep_dataframe_taiko,
    prep_dataframe_opchain,
    prep_dataframe_old,
    get_latest_block,
    get_chain_config
)

CHAIN_PREP_FUNCTIONS = {
    'linea': prep_dataframe_linea,
    'scroll': prep_dataframe_scroll,
    'arbitrum': prep_dataframe_arbitrum,
    'polygon_zkevm': prep_dataframe_polygon_zkevm,
    'zksync_era': prep_dataframe_zksync_era,
    'ethereum': prep_dataframe_eth,
    'taiko': prep_dataframe_taiko,
    
    'zora': prep_dataframe_opchain,
    'base': prep_dataframe_opchain,
    'mode': prep_dataframe_opchain,
    'gitcoin_pgn': prep_dataframe_opchain,
    'mantle': prep_dataframe_opchain,
    'optimism': prep_dataframe_opchain,
    'blast': prep_dataframe_opchain,
    'redstone': prep_dataframe_opchain,
    'orderly': prep_dataframe_opchain,
    'karak': prep_dataframe_opchain,
    'ancient8': prep_dataframe_opchain,
    'kroma': prep_dataframe_opchain,
    'fraxtal': prep_dataframe_opchain,
    'cyber': prep_dataframe_opchain,
    
    'metis': prep_dataframe_old,
    'manta': prep_dataframe_old,
}

def compare_dataframes(df_old, df_new):
    # Check if the DataFrames have the same columns
    if not df_old.columns.equals(df_new.columns):
        
        missing_in_new = set(df_old.columns) - set(df_new.columns)
        if missing_in_new:
            print(f"Columns in the old DataFrame but missing in the new DataFrame: {missing_in_new}")
        
        missing_in_old = set(df_new.columns) - set(df_old.columns)
        if missing_in_old:
            print(f"Columns in the new DataFrame but missing in the old DataFrame: {missing_in_old}")
        
        return False

    # Check if the DataFrames have the same data types
    if not df_old.dtypes.equals(df_new.dtypes):
        print("The DataFrames have different data types.")
        data_type_diff = df_old.dtypes.compare(df_new.dtypes)
        print("Data type differences:\n", data_type_diff)
        return False

    # Separate numeric and non-numeric columns
    numeric_columns = df_old.select_dtypes(include=[np.number]).columns
    non_numeric_columns = df_old.columns.difference(numeric_columns)

    # Compare numeric columns with tolerance for floating-point differences
    tolerance = 1e-10  
    df_numeric_diff = ~np.isclose(df_old[numeric_columns], df_new[numeric_columns], atol=tolerance) & ~(df_old[numeric_columns].isnull() & df_new[numeric_columns].isnull())

    # Compare non-numeric columns directly
    df_non_numeric_diff = (df_old[non_numeric_columns] != df_new[non_numeric_columns]) & ~(df_old[non_numeric_columns].isnull() & df_new[non_numeric_columns].isnull())

    # Combine numeric and non-numeric differences
    df_diff = df_numeric_diff.combine_first(df_non_numeric_diff)

    if df_diff.any().any():
        print("Differences found in the DataFrames.")

        diff_indices = df_diff.any(axis=1)
        print(f"Rows with differences: {df_old[diff_indices].index.tolist()}")

        diff_columns = df_diff.any(axis=0)
        print(f"Columns with differences: {df_diff.columns[diff_columns].tolist()}")
        
        print("Sample of differences in old DataFrame:")
        print(df_old.loc[diff_indices, diff_columns].head())

        print("Sample of differences in new DataFrame:")
        print(df_new.loc[diff_indices, diff_columns].head())

        return False
    else:
        return True

def fetch_and_compare(current_start, current_end, chain, w3):
    df = fetch_data_for_range(w3, current_start, current_end)
    if df is None or df.empty:
        print(f"...skipping blocks {current_start} to {current_end} due to no data.")
        return

    prep_func_old = CHAIN_PREP_FUNCTIONS.get(chain, prep_dataframe_old)
    df_prep_old = prep_func_old(df.copy())

    df_prep_new = prep_dataframe_new(df.copy(), chain)

    for df_prep in [df_prep_old, df_prep_new]:
        df_prep.drop_duplicates(subset=['tx_hash'], inplace=True)
        df_prep.set_index('tx_hash', inplace=True)
        df_prep.index.name = 'tx_hash'

    df_prep_old = df_prep_old.sort_index(axis=0).sort_index(axis=1)
    df_prep_new = df_prep_new.sort_index(axis=0).sort_index(axis=1)

    df_prep_old.reset_index(inplace=True)
    df_prep_new.reset_index(inplace=True)

    are_identical = compare_dataframes(df_prep_old, df_prep_new)

    if are_identical:
        print(f"Success: The old and new preparation functions produce identical results for chain '{chain}' in blocks {current_start} to {current_end}.")
    else:
        print(f"Failure: The old and new preparation functions produce different results for chain '{chain}' in blocks {current_start} to {current_end}.")

def check_chain(chain_name, db_connector):
    try:
        rpc_list, _ = get_chain_config(db_connector, chain_name)
        rpc_urls = [rpc['url'] for rpc in rpc_list]

        if not rpc_urls:
            print(f"No RPC URLs found for chain '{chain_name}'.")
            return

        # Attempt to connect using one RPC at a time, stop if successful
        for rpc_url in rpc_urls:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if w3.is_connected():
                    print(f"==> Successfully connected to {chain_name} with RPC URL {rpc_url}")
                    
                    latest_block = get_latest_block(w3)

                    current_start = latest_block - 2
                    current_end = latest_block

                    fetch_and_compare(current_start, current_end, chain_name, w3)
                    break  # Stop trying more RPC URLs after a successful connection
                else:
                    raise Exception(f"Failed to connect to {chain_name} with RPC URL {rpc_url}")
            except Exception as e:
                print(f"Error connecting to RPC URL {rpc_url} for chain '{chain_name}': {e}")

    except MaxWaitTimeExceededException as e:
        print(f"Error fetching chain config for chain '{chain_name}': {e}")
    
def check_all_chains(db_connector):
    chain_names = CHAIN_PREP_FUNCTIONS.keys()
    for chain_name in chain_names:
        check_chain(chain_name, db_connector)

if __name__ == "__main__":
    db_connector = DbConnector()
    check_all_chains(db_connector)
