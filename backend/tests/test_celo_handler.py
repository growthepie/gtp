
from src.misc.celo_handler import (
    CeloWeb3Provider, 
    print_fee_currencies_and_rates
)
    
print("\nTesting CeloWeb3Provider singleton...")
try:
    web3 = CeloWeb3Provider.get_instance()
    print(f"Connected to Celo network: {web3.is_connected()}")
except Exception as e:
    print(f"Error connecting to Celo network: {e}")

print("\nTesting print_fee_currencies_and_rates...")
try:
    print_fee_currencies_and_rates()
except Exception as e:
    print(f"Error printing fee currencies: {e}")
except ImportError as e:
    print(f"Import error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}") 