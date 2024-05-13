import pandas as pd
import os
from web3 import Web3, HTTPProvider
from sqlalchemy import exc
import threading
from web3.middleware import geth_poa_middleware
import os
import sqlalchemy as sa

from backend.src.new_setup.utils import create_db_engine, get_latest_block, load_environment

def connect_to_node(url):
    w3 = Web3(HTTPProvider(url))
    
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    
    if w3.is_connected():
        return w3
    else:
        return None
        
def fetch_rpc_urls(db_engine, chain_name):
    query = f"""
    SELECT url
    FROM sys_rpc_config
    WHERE origin_key = '{chain_name}';
    """
    try:
        with db_engine.connect() as conn:
            result = pd.read_sql(query, conn)
        print(f"Data fetched successfully for chain: {chain_name}")
        return result
    except exc.SQLAlchemyError as e:
        print(f"Error fetching data for chain: {chain_name}")
        print(e)
        return pd.DataFrame()
    
def fetch_block(url, results):
    try:
        web3_instance = connect_to_node(url)
        if web3_instance is not None:
            block = get_latest_block(web3_instance)
        else:
            block = None
    except Exception as e:
        print(f"Failed to connect to {url}: {str(e)}")
        block = None

    results[url] = block if block is not None else 0


def fetch_all_blocks(rpc_urls):
    threads = []
    results = {}
    for index, rpc in rpc_urls.iterrows():
        thread = threading.Thread(target=fetch_block, args=(rpc['url'], results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results

def check_sync_state(blocks):
    max_block = max(blocks.values())
    notsynced_nodes = []
    for url, block in blocks.items():
        if block == 0 or max_block - block > 20:
            notsynced_nodes.append(url)
    return notsynced_nodes

def deactivate_behind_nodes(db_engine, chain_name, notsynced_nodes):
    if notsynced_nodes:
        query = """
        UPDATE sys_rpc_config
        SET synced = false
        WHERE origin_key = :origin_key AND url IN :urls;
        """
        try:
            with db_engine.begin() as conn:
                conn.execute(sa.text(query), {"origin_key": chain_name, "urls": tuple(notsynced_nodes)})
            print("Nodes set to unsynced.")
        except sa.exc.SQLAlchemyError as e:
            print("Error updating nodes' synced status.")
            print(e)
    else:
        print("No nodes to deactivate.")
          
def get_chains_available(db_engine):
    try:
        with db_engine.connect() as conn:
            query = """
            SELECT DISTINCT origin_key FROM sys_rpc_config;
            """
            result = conn.execute(sa.text(query))
            origin_keys = [row[0] for row in result]
            return origin_keys
    except sa.exc.SQLAlchemyError as e:
        print("Error retrieving unique origin_keys.")
        print(e)
        return []

def activate_nodes(db_engine, chain_name, rpc_urls):
    if not rpc_urls.empty:
        rpc_urls = tuple(rpc_urls['url'].tolist())
        query = """
        UPDATE sys_rpc_config
        SET synced = true
        WHERE origin_key = :origin_key AND url IN :urls;
        """
        try:
            with db_engine.begin() as conn:
                conn.execute(sa.text(query), {"origin_key": chain_name, "urls": rpc_urls})
            print("Nodes set to synced.")
        except sa.exc.SQLAlchemyError as e:
            print("Error updating nodes' synced status.")
            print(e)
    else:
        print("No nodes to activate.")
          
def sync_check():
    db_name, db_user, db_password, db_host, db_port = load_environment()
    db_engine = create_db_engine(db_user, db_password, db_host, db_port, db_name)
    chains = get_chains_available(db_engine)
    for chain_name in chains:
        print(f"Processing chain: {chain_name}")
        rpc_urls = fetch_rpc_urls(db_engine, chain_name)
        activate_nodes(db_engine, chain_name, rpc_urls)
        blocks = fetch_all_blocks(rpc_urls)
        notsynced_nodes = check_sync_state(blocks)
        deactivate_behind_nodes(db_engine, chain_name, notsynced_nodes)
        print(f"Done processing chain: {chain_name}")
        
    db_engine.dispose()
    print("All chains processed.")
        
if __name__ == "__main__":
    sync_check()