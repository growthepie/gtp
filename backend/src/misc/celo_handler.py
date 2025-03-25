from web3 import Web3

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
# Known addresses
# ---------------------------------------------------------------------
FEE_CURRENCY_DIRECTORY_ADDRESS = Web3.to_checksum_address(
    "0x15F344b9E6c3Cb6F0376A36A64928b13F62C6276"
)

# Known tokens on the Celo network
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
# Helper functions
# ---------------------------------------------------------------------
def get_token_symbol(token_address: str) -> str:
    """
    Return the known symbol for a given token address, or 'UNKNOWN' if not found.
    """
    return KNOWN_TOKENS.get(token_address.lower(), "UNKNOWN")


def fetch_token_decimals(web3_instance: Web3, token_address: str, decimals_cache: dict) -> int:
    """
    Return the 'decimals' for an ERC20 token from its on-chain contract.
    Uses an in-memory cache to avoid repeated queries for the same address.
    Defaults to 18 decimals if the contract call fails for any reason.
    """
    address_lower = token_address.lower()
    if address_lower in decimals_cache:
        return decimals_cache[address_lower]

    default_decimals = 18  # fallback
    try:
        token_contract = web3_instance.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=ERC20_ABI
        )
        token_decimals = token_contract.functions.decimals().call()
        decimals_cache[address_lower] = token_decimals
        return token_decimals
    except Exception:
        decimals_cache[address_lower] = default_decimals
        return default_decimals


# ---------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------
def get_fee_currencies_rates_decimals(web3_instance: Web3):
    """
    Retrieves all fee currency token addresses from the FeeCurrencyDirectory,
    then fetches their exchange rate relative to CELO, as well as ERC20 decimals.

    Returns:
        list of dict: Each dict contains:
            - address (str): token contract address
            - symbol (str): known token symbol (or 'UNKNOWN')
            - decimals (int): token decimals
            - numerator (int or None): exchange rate numerator
            - denominator (int or None): exchange rate denominator
            - rate (float or None): numeric value of CELO per token
    """
    results = []
    decimals_cache = {}  # in-memory cache for token decimals

    try:
        # Initialize the FeeCurrencyDirectory contract
        fee_contract = web3_instance.eth.contract(
            address=FEE_CURRENCY_DIRECTORY_ADDRESS,
            abi=FEE_CURRENCY_DIRECTORY_ABI
        )
        currency_addresses = fee_contract.functions.getCurrencies().call()

        # Initialize the exchange-rate contract
        rate_contract = web3_instance.eth.contract(
            address=FEE_CURRENCY_DIRECTORY_ADDRESS,
            abi=EXCHANGE_RATE_ABI
        )

        for addr in currency_addresses:
            checksum_addr = Web3.to_checksum_address(addr)
            symbol = get_token_symbol(checksum_addr)

            # Fetch decimals
            decimals = fetch_token_decimals(web3_instance, checksum_addr, decimals_cache)

            # Fetch exchange rate
            numerator = None
            denominator = None
            rate = None
            try:
                numerator, denominator = rate_contract.functions.getExchangeRate(checksum_addr).call()
                if denominator and denominator != 0:
                    rate = numerator / denominator
            except Exception as exc:
                print(f"Error fetching exchange rate for {symbol} ({addr}): {exc}")

            results.append({
                "address": checksum_addr.lower(),
                "symbol": symbol,
                "decimals": decimals,
                "numerator": numerator,
                "denominator": denominator,
                "rate": rate
            })

    except Exception as exc:
        print(f"Error fetching fee currencies: {exc}")

    return results

def print_fee_currencies_and_rates_and_decimals(web3_instance: Web3):
    data = get_fee_currencies_rates_decimals(web3_instance)
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