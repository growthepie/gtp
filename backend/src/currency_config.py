# TODO: remove this and map all to [fiat.json](https://raw.githubusercontent.com/growthepie/gtp-frontend/a570d6bf4856df66734eb6b4ddc6e2cdc164a71f/public/dicts/fiat.json)
# Currency Configuration for Non-USD Stablecoins
# This file manages fiat currency mappings and exchange rate sources for converting
# non-USD stablecoin supplies to USD values for consistent reporting.

from typing import Dict, List, Optional

# Fiat currency configuration for exchange rate fetching
FIAT_CURRENCY_CONFIG = {
    "eur": {
        "name": "Euro",
        "symbol": "EUR",
        "coingecko_forex_id": "eur",  # CoinGecko exchange rates API key (https://api.coingecko.com/api/v3/exchange_rates)
        "vs_currency": "usd",
        "description": "European Union Euro"
    },
    "brl": {
        "name": "Brazilian Real", 
        "symbol": "BRL",
        "coingecko_forex_id": "brl",
        "vs_currency": "usd",
        "description": "Brazilian Real"
    },
    "usd": {
        "name": "US Dollar",
        "symbol": "USD", 
        "coingecko_forex_id": "usd",
        "vs_currency": "usd",
        "description": "United States Dollar"
    },
    "chf": {
        "name": "Swiss Franc",
        "symbol": "CHF",
        "coingecko_forex_id": "chf",
        "vs_currency": "usd",
        "description": "Swiss Franc"
    },
    "sgd": {
        "name": "Singapore Dollar",
        "symbol": "SGD",
        "coingecko_forex_id": "sgd",
        "vs_currency": "usd",
        "description": "Singapore Dollar"
    },
    "rub": {
        "name": "Russian Ruble",
        "symbol": "RUB",
        "coingecko_forex_id": "rub",
        "vs_currency": "usd",
        "description": "Russian Ruble"
    },
    "php": {
        "name": "Philippine Peso",
        "symbol": "PHP",
        "coingecko_forex_id": "php",
        "vs_currency": "usd",
        "description": "Philippine Peso"
    },
    "aud": {
        "name": "Australian Dollar",
        "symbol": "AUD",
        "coingecko_forex_id": "aud",
        "vs_currency": "usd",
        "description": "Australian Dollar"
    },
    "gbp": {
        "name": "British Pound Sterling",
        "symbol": "GBP",
        "coingecko_forex_id": "gbp",
        "vs_currency": "usd",
        "description": "British Pound Sterling"
    },
    "xof": {
        "name": "West African CFA Franc",
        "symbol": "XOF",
        "coingecko_forex_id": "xof",
        "vs_currency": "usd",
        "description": "West African CFA Franc"
    },
    "cad": {
        "name": "Canadian Dollar",
        "symbol": "CAD",
        "coingecko_forex_id": "cad",
        "vs_currency": "usd",
        "description": "Canadian Dollar"
    },
    "cny": {
        "name": "Chinese Yuan",
        "symbol": "CNY",
        "coingecko_forex_id": "cny",
        "vs_currency": "usd",
        "description": "Chinese Yuan Renminbi"
    },
    "czk": {
        "name": "Czech Koruna",
        "symbol": "CZK",
        "coingecko_forex_id": "czk",
        "vs_currency": "usd",
        "description": "Czech Koruna"
    },
    "dkk": {
        "name": "Danish Krone",
        "symbol": "DKK",
        "coingecko_forex_id": "dkk",
        "vs_currency": "usd",
        "description": "Danish Krone"
    },
    "hkd": {
        "name": "Hong Kong Dollar",
        "symbol": "HKD",
        "coingecko_forex_id": "hkd",
        "vs_currency": "usd",
        "description": "Hong Kong Dollar"
    },
    "huf": {
        "name": "Hungarian Forint",
        "symbol": "HUF",
        "coingecko_forex_id": "huf",
        "vs_currency": "usd",
        "description": "Hungarian Forint"
    },
    "idr": {
        "name": "Indonesian Rupiah",
        "symbol": "IDR",
        "coingecko_forex_id": "idr",
        "vs_currency": "usd",
        "description": "Indonesian Rupiah"
    },
    "ils": {
        "name": "Israeli New Shekel",
        "symbol": "ILS",
        "coingecko_forex_id": "ils",
        "vs_currency": "usd",
        "description": "Israeli New Shekel"
    },
    "inr": {
        "name": "Indian Rupee",
        "symbol": "INR",
        "coingecko_forex_id": "inr",
        "vs_currency": "usd",
        "description": "Indian Rupee"
    },
    "jpy": {
        "name": "Japanese Yen",
        "symbol": "JPY",
        "coingecko_forex_id": "jpy",
        "vs_currency": "usd",
        "description": "Japanese Yen"
    },
    "krw": {
        "name": "South Korean Won",
        "symbol": "KRW",
        "coingecko_forex_id": "krw",
        "vs_currency": "usd",
        "description": "South Korean Won"
    },
    "mxn": {
        "name": "Mexican Peso",
        "symbol": "MXN",
        "coingecko_forex_id": "mxn",
        "vs_currency": "usd",
        "description": "Mexican Peso"
    },
    "myr": {
        "name": "Malaysian Ringgit",
        "symbol": "MYR",
        "coingecko_forex_id": "myr",
        "vs_currency": "usd",
        "description": "Malaysian Ringgit"
    },
    "nok": {
        "name": "Norwegian Krone",
        "symbol": "NOK",
        "coingecko_forex_id": "nok",
        "vs_currency": "usd",
        "description": "Norwegian Krone"
    },
    "nzd": {
        "name": "New Zealand Dollar",
        "symbol": "NZD",
        "coingecko_forex_id": "nzd",
        "vs_currency": "usd",
        "description": "New Zealand Dollar"
    },
    "pln": {
        "name": "Polish Zloty",
        "symbol": "PLN",
        "coingecko_forex_id": "pln",
        "vs_currency": "usd",
        "description": "Polish Zloty"
    },
    "ron": {
        "name": "Romanian Leu",
        "symbol": "RON",
        "coingecko_forex_id": "ron",
        "vs_currency": "usd",
        "description": "Romanian Leu"
    },
    "sek": {
        "name": "Swedish Krona",
        "symbol": "SEK",
        "coingecko_forex_id": "sek",
        "vs_currency": "usd",
        "description": "Swedish Krona"
    },
    "thb": {
        "name": "Thai Baht",
        "symbol": "THB",
        "coingecko_forex_id": "thb",
        "vs_currency": "usd",
        "description": "Thai Baht"
    },
    "try": {
        "name": "Turkish Lira",
        "symbol": "TRY",
        "coingecko_forex_id": "try",
        "vs_currency": "usd",
        "description": "Turkish Lira"
    },
    "zar": {
        "name": "South African Rand",
        "symbol": "ZAR",
        "coingecko_forex_id": "zar",
        "vs_currency": "usd",
        "description": "South African Rand"
    },
    "kes": {
        "name": "Kenyan Shilling",
        "symbol": "KES",
        "coingecko_forex_id": "kes",
        "vs_currency": "usd",
        "description": "Kenyan Shilling"
    },
    "cop": {
        "name": "Colombian Peso",
        "symbol": "COP",
        "coingecko_forex_id": "cop",
        "vs_currency": "usd",
        "description": "Colombian Peso"
    },
    "aed": {
        "name": "United Arab Emirates Dirham",
        "symbol": "AED",
        "coingecko_forex_id": "aed",
        "vs_currency": "usd",
        "description": "United Arab Emirates Dirham"
    },
    "ars": {
        "name": "Argentine Peso",
        "symbol": "ARS",
        "coingecko_forex_id": "ars",
        "vs_currency": "usd",
        "description": "Argentine Peso"
    },
    "bdt": {
        "name": "Bangladeshi Taka",
        "symbol": "BDT",
        "coingecko_forex_id": "bdt",
        "vs_currency": "usd",
        "description": "Bangladeshi Taka"
    },
    "bhd": {
        "name": "Bahraini Dinar",
        "symbol": "BHD",
        "coingecko_forex_id": "bhd",
        "vs_currency": "usd",
        "description": "Bahraini Dinar"
    },
    "bmd": {
        "name": "Bermudian Dollar",
        "symbol": "BMD",
        "coingecko_forex_id": "bmd",
        "vs_currency": "usd",
        "description": "Bermudian Dollar"
    },
    "clp": {
        "name": "Chilean Peso",
        "symbol": "CLP",
        "coingecko_forex_id": "clp",
        "vs_currency": "usd",
        "description": "Chilean Peso"
    },
    "gel": {
        "name": "Georgian Lari",
        "symbol": "GEL",
        "coingecko_forex_id": "gel",
        "vs_currency": "usd",
        "description": "Georgian Lari"
    },
    "kwd": {
        "name": "Kuwaiti Dinar",
        "symbol": "KWD",
        "coingecko_forex_id": "kwd",
        "vs_currency": "usd",
        "description": "Kuwaiti Dinar"
    },
    "lkr": {
        "name": "Sri Lankan Rupee",
        "symbol": "LKR",
        "coingecko_forex_id": "lkr",
        "vs_currency": "usd",
        "description": "Sri Lankan Rupee"
    },
    "mmk": {
        "name": "Burmese Kyat",
        "symbol": "MMK",
        "coingecko_forex_id": "mmk",
        "vs_currency": "usd",
        "description": "Burmese Kyat"
    },
    "ngn": {
        "name": "Nigerian Naira",
        "symbol": "NGN",
        "coingecko_forex_id": "ngn",
        "vs_currency": "usd",
        "description": "Nigerian Naira"
    },
    "pkr": {
        "name": "Pakistani Rupee",
        "symbol": "PKR",
        "coingecko_forex_id": "pkr",
        "vs_currency": "usd",
        "description": "Pakistani Rupee"
    },
    "sar": {
        "name": "Saudi Riyal",
        "symbol": "SAR",
        "coingecko_forex_id": "sar",
        "vs_currency": "usd",
        "description": "Saudi Riyal"
    },
    "twd": {
        "name": "New Taiwan Dollar",
        "symbol": "TWD",
        "coingecko_forex_id": "twd",
        "vs_currency": "usd",
        "description": "New Taiwan Dollar"
    },
    "uah": {
        "name": "Ukrainian Hryvnia",
        "symbol": "UAH",
        "coingecko_forex_id": "uah",
        "vs_currency": "usd",
        "description": "Ukrainian Hryvnia"
    },
    "vef": {
        "name": "Venezuelan Bolívar Fuerte",
        "symbol": "VEF",
        "coingecko_forex_id": "vef",
        "vs_currency": "usd",
        "description": "Venezuelan Bolívar Fuerte"
    },
    "vnd": {
        "name": "Vietnamese Đồng",
        "symbol": "VND",
        "coingecko_forex_id": "vnd",
        "vs_currency": "usd",
        "description": "Vietnamese Đồng"
    },
    "dop": {
        "name": "Dominican Peso",
        "symbol": "DOP",
        "coingecko_forex_id": "dop",
        "vs_currency": "usd",
        "description": "Dominican Peso"
    },
    "crc": {
        "name": "Costa Rican Colón",
        "symbol": "CRC",
        "coingecko_forex_id": "crc",
        "vs_currency": "usd",
        "description": "Costa Rican Colón"
    },
    "hnl": {
        "name": "Honduran Lempira",
        "symbol": "HNL",
        "coingecko_forex_id": "hnl",
        "vs_currency": "usd",
        "description": "Honduran Lempira"
    },
    "zmw": {
        "name": "Zambian Kwacha",
        "symbol": "ZMW",
        "coingecko_forex_id": "zmw",
        "vs_currency": "usd",
        "description": "Zambian Kwacha"
    },
    "svc": {
        "name": "Salvadoran Colón",
        "symbol": "SVC",
        "coingecko_forex_id": "svc",
        "vs_currency": "usd",
        "description": "Salvadoran Colón"
    },
    "bam": {
        "name": "Bosnia and Herzegovina Convertible Mark",
        "symbol": "BAM",
        "coingecko_forex_id": "bam",
        "vs_currency": "usd",
        "description": "Bosnia and Herzegovina Convertible Mark"
    },
    "pen": {
        "name": "Peruvian Sol",
        "symbol": "PEN",
        "coingecko_forex_id": "pen",
        "vs_currency": "usd",
        "description": "Peruvian Sol"
    },
    "gtq": {
        "name": "Guatemalan Quetzal",
        "symbol": "GTQ",
        "coingecko_forex_id": "gtq",
        "vs_currency": "usd",
        "description": "Guatemalan Quetzal"
    },
    "lbp": {
        "name": "Lebanese Pound",
        "symbol": "LBP",
        "coingecko_forex_id": "lbp",
        "vs_currency": "usd",
        "description": "Lebanese Pound"
    },
    "amd": {
        "name": "Armenian Dram",
        "symbol": "AMD",
        "coingecko_forex_id": "amd",
        "vs_currency": "usd",
        "description": "Armenian Dram"
    },
    # not on CoinGecko's exchange_rates: rates come from the GitHub currency API fallback
    "ghs": {
        "name": "Ghanaian Cedi",
        "symbol": "GHS",
        "coingecko_forex_id": "ghs",
        "vs_currency": "usd",
        "description": "Ghanaian Cedi"
    },
    "bob": {
        "name": "Bolivian Boliviano",
        "symbol": "BOB",
        "coingecko_forex_id": "bob",
        "vs_currency": "usd",
        "description": "Bolivian Boliviano"
    },
    "pyg": {
        "name": "Paraguayan Guarani",
        "symbol": "PYG",
        "coingecko_forex_id": "pyg",
        "vs_currency": "usd",
        "description": "Paraguayan Guarani"
    },
    "uyu": {
        "name": "Uruguayan Peso",
        "symbol": "UYU",
        "coingecko_forex_id": "uyu",
        "vs_currency": "usd",
        "description": "Uruguayan Peso"
    },
    "ves": {
        "name": "Venezuelan Bolivar",
        "symbol": "VES",
        "coingecko_forex_id": "ves",
        "vs_currency": "usd",
        "description": "Venezuelan Bolivar Soberano"
    },
}

# Exchange rate API endpoints
EXCHANGE_RATE_APIS = {
    "coingecko": {
        "url": "https://api.coingecko.com/api/v3/exchange_rates",
        "method": "GET",
        "timeout": 10,
        "description": "CoinGecko exchange rates (BTC-relative)"
    },
    "exchangerate_api": {
        "url": "https://api.exchangerate-api.com/v4/latest/USD", 
        "method": "GET",
        "timeout": 10,
        "description": "ExchangeRate-API (USD-based rates)"
    },
    # Historical: Frankfurter API (ECB-backed)
    "frankfurter": {
        "url_template": "https://api.frankfurter.app/{date}?from=USD",
        "method": "GET",
        "timeout": 10,
        "description": "Frankfurter historical USD-based rates"
    },
    # Historical: Guy on Github https://github.com/fawazahmed0/exchange-api
    "github_exchange_api": {
        "url_template": "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json",
        "method": "GET",
        "timeout": 10,
        "description": "Github historical exchange rates"
    }
}

def get_supported_currencies() -> List[str]:
    """
    Get list of supported fiat currencies.
    
    Returns:
        List[str]: List of supported currency codes
    """
    return list(FIAT_CURRENCY_CONFIG.keys())


def get_coingecko_exchange_rate_url() -> str:
    """
    Get primary exchange rate API URL.
    
    Returns:
        str: CoinGecko exchange rates API URL
    """
    return EXCHANGE_RATE_APIS["coingecko"]["url"]

def get_historical_exchange_rate_url(date_str: str) -> str:
    """
    Get historical exchange rate API URL for a specific date (YYYY-MM-DD).
    Uses Frankfurter with USD base.
    """
    return EXCHANGE_RATE_APIS["frankfurter"]["url_template"].format(date=date_str)

def get_github_historical_exchange_rate_url(date_str: str) -> str:
    """
    Get historical exchange rate API URL for a specific date (YYYY-MM-DD).
    Uses Github API with USD base.
    """
    return EXCHANGE_RATE_APIS["github_exchange_api"]["url_template"].format(date=date_str)

def calculate_forex_rate_from_coingecko(base_currency: str, target_currency: str, rates_data: dict) -> Optional[float]:
    """
    Calculate forex rate from CoinGecko exchange rates data.
    
    CoinGecko rates are relative to Bitcoin (BTC = 1.0), so to get EUR/USD:
    EUR/USD = USD_rate / EUR_rate
    
    Args:
        base_currency (str): Base currency code (e.g., 'eur')  
        target_currency (str): Target currency code (e.g., 'usd')
        rates_data (dict): CoinGecko rates response data
        
    Returns:
        Optional[float]: Exchange rate or None if calculation fails
    """
    try:
        if base_currency == target_currency:
            return 1.0
            
        rates = rates_data.get('rates', {})
        
        base_rate = rates.get(base_currency.lower(), {}).get('value')
        target_rate = rates.get(target_currency.lower(), {}).get('value')
        
        if base_rate is None or target_rate is None:
            return None
            
        # Calculate rate: base/target = target_rate / base_rate
        return target_rate / base_rate
        
    except (KeyError, ZeroDivisionError, TypeError):
        return None

# Error handling configuration
RATE_FETCH_CONFIG = {
    "max_retries": 3,
    "retry_delay": 1.0,  # seconds
    "timeout": 10.0,     # seconds
}


