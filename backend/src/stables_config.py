# Token metadata
stables_metadata = {
    "usdc": {
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "coingecko_id": "usd-coin",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694",
        "addresses": {
            "ethereum": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        }
    },
    "tether": {
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "coingecko_id": "tether",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/325/large/Tether.png?1696501661",
        "addresses": {
            "ethereum": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        }
    },
    "ethena-usde": {
        "name": "Ethena USDe",
        "symbol": "USDe",
        "decimals": 18,
        "coingecko_id": "ethena-usde",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/33613/large/usde.png?1733810059",
        "addresses": {
            "ethereum": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
        }
    },
}


# Layer 2 bridge or direct token mapping
## bridged: locked value is calculated based on bridge contracts on different chains
### Check for any stable that we track if it is locked in here

## direct: token is directly minted on Layer 2
### Call method to get total supply of the token on the chain

stables_mapping = {
    "swell": {
        "bridged": {
            "ethereum": [
                "0x7aA4960908B13D104bf056B23E2C76B43c5AACc8" ##proxy, 0 tokens in it on March 3rd, 2025
            ], 
        },
        "direct": {
            "ethena-usde": {
                "token_address" : "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            },
            "tether": {
                "token_address" : "0xb89c6ED617f5F46175E41551350725A09110bbCE",
                "method_name": "totalSupply",
            }
            ## staked ethena-staked-usde??
        }
    },
    "soneium": {
        "bridged": {
            "ethereum": [
                "0xeb9bf100225c214Efc3E7C651ebbaDcF85177607", ## Generic escrow contract
                "0xC67A8c5f22b40274Ca7C4A56Db89569Ee2AD3FAb" ## Escrow for USDC
            ],
        },
        "direct": {

        }
    },
}

