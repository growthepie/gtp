from tests import setup_test_imports
# Set up imports
setup_test_imports()

import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3

from src.adapters.adapter_total_supply import AdapterTotalSupply
from src.db_connector import DbConnector
from src.main_config import get_main_config

def main():
    print("Testing specific changes to AdapterTotalSupply implementation...")
    
    # Create DB connector
    db_connector = DbConnector()
    
    # Create adapter with required parameters
    adapter_params = {
    }
    
    adapter = AdapterTotalSupply(adapter_params, db_connector)
    
    # Test 1: Testing with zksync_era which should use hardcoded total supply value
    print("\nTest 1: Testing zksync_era which should use hardcoded total supply (21,000,000,000)")
    
    load_params = {
        'origin_keys': ['zksync_era'],
        'days': 1
    }
    
    try:
        df = adapter.extract(load_params)
        print(f"\nExtract succeeded with DataFrame shape: {df.shape}")
        if not df.empty:
            print("Sample data:")
            display_df = df.reset_index().head()
            print(display_df)
            
            # Verify the hardcoded value of 21,000,000,000 was used
            if 'value' in display_df.columns and display_df['value'].iloc[0] == 21000000000:
                print("\n✅ Confirmed hardcoded value of 21,000,000,000 used for zksync_era")
            else:
                print("\n❌ Expected hardcoded value not found")
        else:
            print("DataFrame is empty.")
    except Exception as e:
        print(f"Error during extract: {e}")
    
    # Test 2: Testing with another chain that uses hardcoded value - celo
    print("\nTest 2: Testing celo which should use hardcoded total supply (1,000,000,000)")
    
    load_params_celo = {
        'origin_keys': ['celo'],
        'days': 1
    }
    
    try:
        df_celo = adapter.extract(load_params_celo)
        print(f"\nExtract succeeded with DataFrame shape: {df_celo.shape}")
        if not df_celo.empty:
            print("Sample data:")
            display_df = df_celo.reset_index().head()
            print(display_df)
            
            # Verify the hardcoded value of 1,000,000,000 was used
            if 'value' in display_df.columns and display_df['value'].iloc[0] == 1000000000:
                print("\n✅ Confirmed hardcoded value of 1,000,000,000 used for celo")
            else:
                print("\n❌ Expected hardcoded value not found")
        else:
            print("DataFrame is empty.")
    except Exception as e:
        print(f"Error during extract: {e}")
    
    # Test 3: Test with optimism to verify the hardcoded ABI works for real token contracts
    print("\nTest 3: Testing optimism with real token contract using hardcoded ABI")
    
    # First check if we have the required data in the fact_kpis table
    block_data = db_connector.get_data_from_table(
        "fact_kpis",
        filters={
            "metric_key": "first_block_of_day",
            "origin_key": "optimism"
        },
        days=1
    )
    
    if block_data.empty:
        print("Warning: No block data found in fact_kpis table for optimism. Testing manual contract interaction instead.")
        # Get main config to find token addresses
        main_conf = get_main_config()
        optimism_config = next((chain for chain in main_conf if chain.origin_key == "optimism"), None)
        
        if optimism_config and optimism_config.cs_token_address:
            token_address = optimism_config.cs_token_address
            print(f"Using token address: {token_address}")
            
            # Get a valid RPC endpoint
            rpc = db_connector.get_special_use_rpc('optimism')
            print(f"Using RPC: {rpc}")
            
            # Test with the hardcoded ABI directly
            token_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "totalSupply",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                }
            ]
            
            try:
                # Set up web3 and contract
                w3 = Web3(Web3.HTTPProvider(rpc))
                token_address = token_address if Web3.is_checksum_address(token_address) else Web3.to_checksum_address(token_address)
                contract = w3.eth.contract(address=token_address, abi=token_abi)
                
                # Try to call decimals() and totalSupply() functions
                decimals = contract.functions.decimals().call()
                total_supply = contract.functions.totalSupply().call()
                
                print(f"Successfully called decimals(): {decimals}")
                print(f"Successfully called totalSupply(): {total_supply / 10**decimals}")
                print("✅ Hardcoded ABI test passed!")
            except Exception as e:
                print(f"Error testing hardcoded ABI: {e}")
        else:
            print("Could not find optimism token address in config.")
    else:
        print(f"Found {len(block_data)} block records in fact_kpis table.")
        print(block_data.head())
        
        # Run extract for optimism
        load_params_optimism = {
            'origin_keys': ['optimism'],
            'days': 1
        }
        
        try:
            df_optimism = adapter.extract(load_params_optimism)
            print(f"\nExtract succeeded with DataFrame shape: {df_optimism.shape}")
            if not df_optimism.empty:
                print("Sample data:")
                display_df = df_optimism.reset_index().head()
                print(display_df)
                print("✅ Successfully extracted total supply for optimism using hardcoded ABI")
            else:
                print("DataFrame is empty.")
        except Exception as e:
            print(f"Error during extract: {e}")
    
    # Test 4: List available chains to help understand what we can test with
    print("\nTest 4: Listing available chains in main_config")
    
    main_conf = get_main_config()
    available_chains = [chain.origin_key for chain in main_conf if hasattr(chain, 'origin_key')]
    chains_with_tokens = [chain.origin_key for chain in main_conf if hasattr(chain, 'cs_token_address') and chain.cs_token_address is not None]
    
    print(f"Available chains: {available_chains[:10]}...") # Show first 10 chains
    print(f"Chains with token addresses: {chains_with_tokens}")
        
    print("\nTest completed.")

if __name__ == "__main__":
    main() 