# Token metadata
# a dictionary of tokens that can be considered stablecoins
# each token has a name, symbol, decimals, coingecko_id, fiat currency, logo and addresses
# the addresses are the contract addresses for the token on the ethereum network

# name: the name is the name of the token
# symbol: the symbol is the ticker symbol of the token
# decimals: the number of decimals the token has
# coingecko_id: the coingecko_id is the id of the token on coingecko
# fiat: the fiat currency is the currency that the token is pegged to
# logo: (optional) the logo is a link to the token's logo 
# addresses: the addresses are the contract addresses for the token on the Ethereum network

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
    "dai": {
        "name": "Dai",
        "symbol": "DAI",
        "decimals": 18,
        "coingecko_id": "dai",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/9956/large/Badge_Dai.png?1696509996",
        "addresses": {
            "ethereum": "0x6b175474e89094c44da98b954eedeac495271d0f",
        }
    },
    "usds": {
        "name": "USDS (former DAI)",
        "symbol": "USDS",
        "decimals": 18,
        "coingecko_id": "usds",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/39926/large/usds.webp?1726666683",
        "addresses": {
            "ethereum": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
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
    "bincance_usd": {
        "name": "Binance USD",
        "symbol": "BUSD",
        "decimals": 18,
        "coingecko_id": "binance-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/9576/large/BUSDLOGO.jpg?1696509654",
        "addresses": {
            "ethereum": "0x4fabb145d64652a948d72533023f6e7a623c7c53",
        }
    },
    "true_usd": {
        "name": "TrueUSD",
        "symbol": "TUSD",
        "decimals": 18,
        "coingecko_id": "true-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/3449/large/tusd.png?1696504140",
        "addresses": {
            "ethereum": "0x0000000000085d4780b73119b644ae5ecd22b376",
        }
    },
    "frax": {
        "name": "Frax",
        "symbol": "FRAX",
        "decimals": 18,
        "coingecko_id": "frax",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/13422/large/FRAX_icon.png?1696513182",
        "addresses": {
            "ethereum": "0x853d955acef822db058eb8505911ed77f175b99e",
        }
    },
    "pax-dollar": {
        "name": "Pax Dollar",
        "symbol": "USDP",
        "decimals": 18,
        "coingecko_id": "paxos-standard",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6013/large/Pax_Dollar.png?1696506427",
        "addresses": {
            "ethereum": "0x8e870d67f660d95d5be530380d0ec0bd388289e1",
        }
    },
    "gemini-usd": {
        "name": "Gemini Dollar",
        "symbol": "GUSD",
        "decimals": 2,
        "coingecko_id": "gemini-dollar",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/5992/large/gemini-dollar-gusd.png?1696506408",
        "addresses": {
            "ethereum": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
        }
    },
    "paypal-usd": {
        "name": "PayPal USD",
        "symbol": "PYUSD",
        "decimals": 18,
        "coingecko_id": "paypal-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        }
    },
    "liquity-usd": {
        "name": "Liquity USD",
        "symbol": "LUSD",
        "decimals": 18,
        "coingecko_id": "liquity-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/14666/large/Group_3.png?1696514341",
        "addresses": {
            "ethereum": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
        }
    },
    "mountain-protocol-usdm": {
        "name": "Mountain Protocol USD",
        "symbol": "USDM",
        "decimals": 18,
        "coingecko_id": "mountain-protocol-usdm",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/31719/large/usdm.png?1696530540",
        "addresses": {
            "ethereum": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
        }
    },
    "izumi-bond-usd": {
        "name": "iZUMi Bond USD",
        "symbol": "IUSD",
        "decimals": 18,
        "coingecko_id": "izumi-bond-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/25388/large/iusd-logo-symbol-10k%E5%A4%A7%E5%B0%8F.png?1696524521",
        "addresses": {
            "ethereum": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
        }
    },
    "electronic-usd": {
        "name": "Electronic USD",
        "symbol": "eUSD",
        "decimals": 18,
        "coingecko_id": "electronic-usd",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/28445/large/0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f.png?1696527441",
        "addresses": {
            "ethereum": "0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f",
        }
    },
    "curve-usd": {
        "name": "Curve USD",
        "symbol": "crvUSDC",
        "decimals": 18,
        "coingecko_id": "crvusd",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/30118/large/crvusd.jpeg?1696529040",
        "addresses": {
            "ethereum": "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e",
        }
    },
    "dola": {
        "name": "Dola",
        "symbol": "DOLA",
        "decimals": 18,
        "coingecko_id": "dola-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x865377367054516e17014ccded1e7d814edc9ce4",
        }
    },
    "alchemix-usd": {
        "name": "Alchemix USD",
        "symbol": "ALUSD",
        "decimals": 18,
        "coingecko_id": "alchemix-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/14114/large/Alchemix_USD.png?1696513835",
        "addresses": {
            "ethereum": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",
        }
    },
    "first-digital-usd": {
        "name": "First Digital USD",
        "symbol": "FDUSD",
        "decimals": 18,
        "coingecko_id": "first-digital-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
        }
    },
    "usual-usd": {
        "name": "Usual USD",
        "symbol": "USD0",
        "decimals": 18,
        "coingecko_id": "usual-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5",
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
    },
    "base": {},
    "arbitrum": {},
    "arbitrum_nova": {
        "bridged": {
            "ethereum": [
                "0xB2535b988dcE19f9D71dfB22dB6da744aCac21bf", # Arbitrum Nova L1 ERC20 Gateway
                "0x23122da8C581AA7E0d07A36Ff1f16F799650232f", # Arbitrum Nova: L1 Arb-Custom Gateway
                "0xA2e996f0cb33575FA0E36e8f62fCd4a9b897aAd3" # DAI Escrow contract
            ], 
        },
    },
    "optimism": {},

    "taiko": {},
    "linea": {},
    "mantle": {},
    "ink": {},
    "zksync_era": {},
    "worldchain": {},
    "manta": {},
    "scroll": {},
    "blast": {},
    "mode": {},
    "real": {},
    "redstone": {},
    "unichain": {
        "bridged": {
            "ethereum": [
                "0x81014F44b0a345033bB2b3B21C7a1A308B35fEeA"  # Bridge contract locking USDT and DAI for Unichain
            ]
        },
        "direct": {
            "usdc": {
                "token_address": "0x078D782b760474a361dDA0AF3839290b0EF57AD6",  # USDC native on Unichain
                "method_name": "totalSupply",
            }
        }
    },
    #"gravity": {},
    #"mint": {},
    #"metis": {},
    #"zora": {},
    #"starknet": {},
    #"imx": {},
    #"polygon_zkevm": {},
    #"fraxtal": {},
    #"loopring": {},
    #"orderly": {},
    #"rhino": {},
    #"derive": {},
}

