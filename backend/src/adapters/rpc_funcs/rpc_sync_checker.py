import pandas as pd
from web3 import Web3, HTTPProvider
from sqlalchemy import exc
import threading
from web3.middleware import ExtraDataToPOAMiddleware
from src.db_connector import DbConnector
import sqlalchemy as sa
import time

from src.adapters.rpc_funcs.utils import get_latest_block

def connect_to_node(url):
    """
    Connects to an Ethereum node at the given URL using Web3.
    Retries the connection up to 5 times in case of failure.

    Args:
        url (str): The URL of the Ethereum node.

    Returns:
        Web3: The connected Web3 instance or None if the connection fails after retries.
    """
    retries = 5
    delay = 5
    w3 = Web3(HTTPProvider(url))
    
    # Inject POA middleware for chains that need it (safe to inject even if not needed)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    
    for attempt in range(1, retries + 1):
        if w3.is_connected():
            return w3
        else:
            if attempt < retries:
                #print(f"...attempt {attempt} failed for {w3.provider.endpoint_uri}, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"...attempt {attempt} failed for {w3.provider.endpoint_uri}. No more retries left.")
    return None
        
def fetch_rpc_urls(db_connector, chain_name):
    """
    Fetches active RPC URLs for a specific blockchain chain from the database.

    Args:
        db_connector: Database connector used to execute the query.
        chain_name (str): The name of the blockchain chain.

    Returns:
        pd.DataFrame: A DataFrame containing the active RPC URLs for the specified chain.
    """
    query = f"""
    SELECT url
    FROM sys_rpc_config
    WHERE origin_key = '{chain_name}'
    AND active = true;
    """
    try:
        with db_connector.engine.connect() as conn:
            result = pd.read_sql(query, conn)
        print(f"...RPC data fetched successfully for chain: {chain_name}")
        return result
    except exc.SQLAlchemyError as e:
        print(f"ERROR: fetching data for chain: {chain_name}")
        print(e)
        return pd.DataFrame()
    
def fetch_block(url, results):
    """
    Fetches the latest block number from the specified Ethereum node URL.
    If the connection fails, it returns 0 for that URL.

    Args:
        url (str): The URL of the Ethereum node.
        results (dict): A dictionary to store the block number for the URL.
    """
    try:
        web3_instance = connect_to_node(url)
        if web3_instance is not None:
            block = get_latest_block(web3_instance)
        else:
            block = None
    except Exception as e:
        print(f"ERROR: Failed to connect to {url}: {str(e)}")
        block = None

    results[url] = block if block is not None else 0


def fetch_all_blocks(rpc_urls):
    """
    Fetches the latest block number from all provided RPC URLs in parallel using threading.

    Args:
        rpc_urls (pd.DataFrame): DataFrame containing the RPC URLs.

    Returns:
        dict: A dictionary mapping each RPC URL to its latest block number.
    """
    threads = []
    results = {}
    for index, rpc in rpc_urls.iterrows():
        thread = threading.Thread(target=fetch_block, args=(rpc['url'], results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results

def check_sync_state(blocks, block_threshold):
    """
    Checks the synchronization state of all nodes by comparing their block heights.
    Identifies nodes that are either too far behind or not responding.

    Args:
        blocks (dict): A dictionary mapping RPC URLs to their block numbers.
        block_threshold (int): The maximum allowed difference between the highest block and a node's block before it is considered unsynced.

    Returns:
        list: A list of URLs for nodes that are unsynced.
    """
    max_block = max(blocks.values())
    notsynced_nodes = []
    for url, block in blocks.items():
        if block == 0:
            print(f"UNSYNCED: Node {url} is not responding (block == 0).")
            notsynced_nodes.append(url)
        elif max_block - block > block_threshold:
            print(f"UNSYNCED: Node {url} is too far behind. Max block: {max_block} // Node block: {block} // Behind by: {max_block - block}")
            notsynced_nodes.append(url)
    return notsynced_nodes

def deactivate_behind_nodes(db_connector, chain_name, notsynced_nodes):
    """
    Deactivates nodes in the database that are not synchronized by marking them as 'unsynced'.

    Args:
        db_connector: Database connector used to execute the query.
        chain_name (str): The name of the blockchain chain.
        notsynced_nodes (list): A list of unsynced node URLs.
    """
    if notsynced_nodes:
        query = """
        UPDATE sys_rpc_config
        SET synced = false
        WHERE origin_key = :origin_key AND url IN :urls;
        """
        try:
            with db_connector.engine.begin() as conn:
                conn.execute(sa.text(query), {"origin_key": chain_name, "urls": tuple(notsynced_nodes)})
            print(f"UNSYNCED Nodes: {tuple(notsynced_nodes)} set to unsynced.")
        except sa.exc.SQLAlchemyError as e:
            print("ERROR: updating nodes' synced status.")
            print(e)
    else:
        print("...no nodes to deactivate.")
          
def get_chains_available(db_connector):
    """
    Retrieves a list of unique blockchain chain names from the database.

    Args:
        db_connector: Database connector used to execute the query.

    Returns:
        list: A list of distinct blockchain chain names.
    """
    try:
        with db_connector.engine.connect() as conn:
            query = """
            SELECT DISTINCT origin_key FROM sys_rpc_config;
            """
            result = conn.execute(sa.text(query))
            origin_keys = [row[0] for row in result]
            return origin_keys
    except sa.exc.SQLAlchemyError as e:
        print("ERROR: retrieving unique origin_keys.")
        print(e)
        return []

def activate_nodes(db_connector, chain_name, rpc_urls):
    """
    Activates nodes in the database by marking them as 'synced'.

    Args:
        db_connector: Database connector used to execute the query.
        chain_name (str): The name of the blockchain chain.
        rpc_urls (pd.DataFrame): A DataFrame containing the URLs of the nodes to activate.
    """
    if not rpc_urls.empty:
        rpc_urls = tuple(rpc_urls['url'].tolist())
        query = """
        UPDATE sys_rpc_config
        SET synced = true
        WHERE origin_key = :origin_key AND url IN :urls;
        """
        try:
            with db_connector.engine.begin() as conn:
                conn.execute(sa.text(query), {"origin_key": chain_name, "urls": rpc_urls})
            print("...nodes set to synced.")
        except sa.exc.SQLAlchemyError as e:
            print("ERROR: updating nodes' synced status.")
            print(e)
    else:
        print("...no nodes to activate.")
          
def sync_check():
    """
    Performs a synchronization check for all blockchain chains.
    Fetches the latest block numbers, checks sync state, activates synchronized nodes, and deactivates unsynced nodes.

    The function sets a block threshold of 100 for 'arbitrum' and 30 for other chains.
    """
    db_connector = DbConnector()

    chains = get_chains_available(db_connector)
    for chain_name in chains:
        if chain_name == 'arbitrum':
            block_threshold = 100
        else:
            block_threshold = 30
            
        print(f"START: processing chain: {chain_name}")
        rpc_urls = fetch_rpc_urls(db_connector, chain_name)

        if rpc_urls.empty:
            print(f"...no RPC urls found for chain: {chain_name} to process.")
        else:
            activate_nodes(db_connector, chain_name, rpc_urls)
            blocks = fetch_all_blocks(rpc_urls)
            notsynced_nodes = check_sync_state(blocks, block_threshold)
            deactivate_behind_nodes(db_connector, chain_name, notsynced_nodes)
        print(f"DONE: processing chain: {chain_name}")
        
    print("FINISHED: All chains processed.")
        
if __name__ == "__main__":
    sync_check()