from typing import Dict, List, Optional, Tuple, Any, Union
from web3 import Web3
from web3.exceptions import ContractLogicError
from src.adapters.rpc_funcs.utils import get_chain_config
from src.db_connector import DbConnector
import time

# ---------------------------------------------------------------------
# Contract ABIs
# ---------------------------------------------------------------------
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

FEE_CURRENCY_DIRECTORY_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "getCurrencies",
        "outputs": [{"name": "", "type": "address[]"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

EXCHANGE_RATE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "token", "type": "address"}],
        "name": "getExchangeRate",
        "outputs": [
            {"name": "numerator", "type": "uint256"},
            {"name": "denominator", "type": "uint256"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# ---------------------------------------------------------------------
# Contract addresses
# ---------------------------------------------------------------------
FEE_CURRENCY_DIRECTORY_ADDRESS = Web3.to_checksum_address(
    "0x15F344b9E6c3Cb6F0376A36A64928b13F62C6276"
)

# ---------------------------------------------------------------------
# Known Celo tokens
# ---------------------------------------------------------------------
KNOWN_TOKENS = {
    "0x471ece3750da237f93b8e339c536989b8978a438": "CELO",
    "0x765de816845861e75a25fca122bb6898b8b1282a": "cUSD",
    "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73": "cEUR",
    "0xe8537a3d056da446677b9e9d6c5db704eaab4787": "cREAL",
    "0x73f93dcc49cb8a239e2032663e9475dd5ef29a08": "eXOF",
    "0x456a3d042c0dbd3db53d5489e98dfb038553b0d0": "cKES",
    "0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b": "PUSO",
    "0x8a567e2ae79ca692bd748ab832081c45de4041ea": "cCOP",
    "0xfaea5f3404bba20d3cc2f8c4b0a888f55a3c7313": "cGHS",
    "0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e": "USDT",
    "0xceba9300f2b948710d2653dd7b07f33a8b32118c": "USDC",
    "0x0e2a3e05bc9a16f5292a6170456a710cb89c6f72": "USDT-Adapter",
    "0x2f25deb3848c207fc8e0c34035b3ba7fc157602b": "USDC-Adapter",
    "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3": "USDGLO",
}

# ---------------------------------------------------------------------
# RPC connection
# ---------------------------------------------------------------------
class CeloWeb3Provider:
    """Manages Web3 connection to Celo"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> Union[Any, Web3]:
        """
        Singleton pattern to get or create a Web3 connection to Celo.
        
        Returns:
            Web3CC: Connected Web3 instance
            
        Raises:
            ConnectionError: If all connection attempts fail
        """
        if cls._instance is None:
            from src.adapters.rpc_funcs.utils import Web3CC
            
            db_connector = DbConnector()
            rpc_configs, _ = get_chain_config(db_connector, 'celo')
            
            for rpc_config in rpc_configs:
                try:
                    cls._instance = Web3CC(rpc_config)
                    print(f"Connected to Celo RPC: {rpc_config['url']}")
                    break
                except Exception as e:
                    print(f"Failed to connect to Celo RPC: {rpc_config['url']} with error: {e}")
            
            if cls._instance is None:
                raise ConnectionError("Failed to connect to any Celo RPC node.")
                
        return cls._instance

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def get_token_symbol(token_address: str) -> str:
    """
    Get the symbol for a given token address.
    
    Args:
        token_address (str): The token contract address
        
    Returns:
        str: Token symbol or 'UNKNOWN' if not in known tokens list
    """
    return KNOWN_TOKENS.get(token_address.lower(), "UNKNOWN")


def fetch_token_decimals(web3_instance: Any, token_address: str, decimals_cache: Dict[str, int]) -> int:
    """
    Fetch the decimals value for an ERC20 token from its contract.
    
    Args:
        web3_instance: Web3 connection instance
        token_address (str): The token contract address
        decimals_cache (dict): Cache of already fetched decimals
        
    Returns:
        int: Token decimals (defaults to 18 if fetch fails)
    """
    address_lower = token_address.lower()
    
    # Return cached value if available
    if address_lower in decimals_cache:
        return decimals_cache[address_lower]

    default_decimals = 18  # Standard default for most tokens
    
    try:
        # Create contract instance and call decimals()
        token_contract = web3_instance.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=ERC20_ABI
        )
        token_decimals = token_contract.functions.decimals().call()
        
        # Cache and return the result
        decimals_cache[address_lower] = token_decimals
        return token_decimals
    except (ContractLogicError, ValueError) as e:
        decimals_cache[address_lower] = default_decimals
        return default_decimals
    except Exception as e:
        print(f"Unexpected error fetching decimals for token {token_address}: {e}")
        decimals_cache[address_lower] = default_decimals
        return default_decimals


def fetch_exchange_rate(
        web3_instance: Any, 
        rate_contract, 
        token_address: str
) -> Tuple[Optional[int], Optional[int], Optional[float]]:
    """
    Fetch exchange rate for a token relative to CELO.
    
    Args:
        web3_instance: Web3 connection instance
        rate_contract: Initialized contract for exchange rate lookup
        token_address (str): The token contract address
        
    Returns:
        tuple: (numerator, denominator, rate) where rate is numerator/denominator
               Returns (None, None, None) if the fetch fails
    """
    try:
        numerator, denominator = rate_contract.functions.getExchangeRate(token_address).call()
        rate = numerator / denominator if denominator and denominator != 0 else None
        return numerator, denominator, rate
    except Exception as e:
        symbol = get_token_symbol(token_address)
        print(f"Error fetching exchange rate for {symbol} ({token_address}): {e}")
        return None, None, None


# ---------------------------------------------------------------------
# Main functions
# ---------------------------------------------------------------------
def get_fee_currencies_rates_decimals(web3_instance: Optional[Any] = None) -> List[Dict[str, Any]]:
    """
    Retrieve fee currency info from Celo contracts including exchange rates and decimals.
    
    This function queries on-chain contracts to get:
    1. The list of fee currencies allowed for gas payments
    2. The exchange rate of each currency relative to CELO
    3. The decimals for each currency token
    
    Args:
        web3_instance (optional): Web3 connection to use. If None, creates one.
        
    Returns:
        list: List of dictionaries with token info:
            - address (str): Contract address
            - symbol (str): Token symbol
            - decimals (int): Token decimal places
            - numerator (int): Exchange rate numerator or None
            - denominator (int): Exchange rate denominator or None
            - rate (float): CELO per token or None
            
    Raises:
        ConnectionError: If web3_instance is None and no RPC connection can be established
    """
    # Use provided web3 instance or get the singleton instance
    if web3_instance is None:
        web3_instance = CeloWeb3Provider.get_instance()
        
    results = []
    decimals_cache = {}  # Cache to avoid duplicate RPC calls

    try:
        # 1. Initialize contracts
        fee_contract = web3_instance.eth.contract(
            address=FEE_CURRENCY_DIRECTORY_ADDRESS,
            abi=FEE_CURRENCY_DIRECTORY_ABI
        )
        rate_contract = web3_instance.eth.contract(
            address=FEE_CURRENCY_DIRECTORY_ADDRESS,
            abi=EXCHANGE_RATE_ABI
        )
        
        # 2. Fetch the list of allowed fee currencies
        currency_addresses = fee_contract.functions.getCurrencies().call()

        # 3. Process each token
        for addr in currency_addresses:
            checksum_addr = Web3.to_checksum_address(addr)
            symbol = get_token_symbol(checksum_addr)
            
            # Get token decimals
            decimals = fetch_token_decimals(web3_instance, checksum_addr, decimals_cache)
            
            # Get exchange rate
            numerator, denominator, rate = fetch_exchange_rate(
                web3_instance, rate_contract, checksum_addr
            )

            # Add to results
            results.append({
                "address": checksum_addr.lower(),
                "symbol": symbol,
                "decimals": decimals,
                "numerator": numerator,
                "denominator": denominator,
                "rate": rate
            })

    except Exception as e:
        print(f"Error fetching Celo fee currencies: {e}")

    return results

def print_fee_currencies_and_rates(web3_instance: Optional[Any] = None) -> None:
    """
    Print a formatted report of all Celo fee currencies and their exchange rates.
    
    Args:
        web3_instance (optional): Web3 connection to use. If None, creates one.
    """
    data = get_fee_currencies_rates_decimals(web3_instance)
    
    print("\n=== Celo Fee Currencies ===")
    for entry in data:
        print(f"Token: {entry['symbol']} ({entry['address']})")
        print(f"  Decimals: {entry['decimals']}")
        
        if entry['rate'] is not None:
            print(f"  Numerator: {entry['numerator']}")
            print(f"  Denominator: {entry['denominator']}")
            print(f"  Rate: {entry['rate']} CELO per {entry['symbol']}")
        else:
            print("  Exchange Rate: Unavailable")
        print()
    print("==========================\n")

class CeloFeeCache:
    """Singleton cache for Celo fee currency data"""
    _instance = None
    _cache = {
        'decimals': {},  # address -> decimals
        'rates': {},     # address -> rate
        'last_update': None
    }
    _cache_duration = 900  # Cache duration in seconds (15 minutes)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CeloFeeCache, cls).__new__(cls)
        return cls._instance

    def _should_refresh_cache(self):
        """Check if cache needs refreshing based on time elapsed"""
        if self._cache['last_update'] is None:
            return True
        
        current_time = time.time()
        return (current_time - self._cache['last_update']) > self._cache_duration

    def refresh_cache(self, web3_instance=None):
        """Refresh the cache with current fee currency data"""
        if web3_instance is None:
            web3_instance = CeloWeb3Provider.get_instance()

        fee_currencies_data = get_fee_currencies_rates_decimals(web3_instance)
        
        # Update cache
        self._cache['decimals'] = {
            entry["address"].lower(): entry["decimals"] 
            for entry in fee_currencies_data
        }
        self._cache['rates'] = {
            entry["address"].lower(): entry["rate"] 
            for entry in fee_currencies_data 
            if entry["rate"] is not None
        }

        # Add CELO defaults
        CELO_ADDRESS = "0x471ece3750da237f93b8e339c536989b8978a438".lower()
        self._cache['decimals'].setdefault(CELO_ADDRESS, 18)
        self._cache['rates'].setdefault(CELO_ADDRESS, 1.0)

        self._cache['last_update'] = time.time()

    def get_cached_data(self, web3_instance=None):
        """Get cached fee currency data, refreshing if necessary"""
        if self._should_refresh_cache():
            self.refresh_cache(web3_instance)
        return self._cache['decimals'], self._cache['rates']

    def force_refresh(self, web3_instance=None):
        """Force a cache refresh regardless of time elapsed"""
        self.refresh_cache(web3_instance)