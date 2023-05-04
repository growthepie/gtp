from pydantic import BaseModel
from typing import Optional

## Adapter Mappings
class AdapterMapping(BaseModel):
    origin_key: str
    name: str
    symbol: Optional[str]
    technology: str ## -, zk, optimistic
    launch_date: Optional[str] ## YYYY-MM-DD
    website: Optional[str]
    block_explorer: Optional[str]
    twitter: Optional[str]

    coingecko_naming: Optional[str] ## to load price, market cap, etc
    l2beat_tvl_naming: Optional[str] ## to load tvl
    defillama_stablecoin : Optional[str] ## to load stablecoin tvl

adapter_mapping = [
    # Layer 1
    AdapterMapping(
        origin_key="ethereum"
        ,name = "Ethereum"
        ,symbol = "ETH"
        ,technology = 'Mainnet'
        ,purpose = 'General Purpose (EVM)'
        ,launch_date = '2015-07-30'
        ,website = 'https://ethereum.org/'
        ,block_explorer = 'https://etherscan.io/'
        ,twitter = 'https://twitter.com/ethereum'

        ,coingecko_naming="ethereum"
        ,defillama_stablecoin="ethereum"
        )
   
    # Layer 2s    
    ,AdapterMapping(
        origin_key="polygon_zkevm"
        ,name = "Polygon zkEVM"
        ,symbol = "MATIC"
        ,technology = "ZK Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-03-24'
        ,website='https://polygon.technology/polygon-zkevm'
        ,block_explorer='https://zkevm.polygonscan.com/'
        ,twitter='https://twitter.com/0xPolygon'

        ,coingecko_naming="matic-network"
        #,defillama_stablecoin=''  ## stables via Flipside
        ,l2beat_tvl_naming='polygonzkevm'
        )
    ,AdapterMapping(
        origin_key="optimism"
        ,name = "Optimism"
        ,symbol = "OP"
        ,technology = "Optimistic Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2021-12-16'
        ,website='https://optimism.io/'
        ,block_explorer='https://optimistic.etherscan.io/'
        ,twitter='https://twitter.com/optimismFND'

        ,coingecko_naming="optimism"
        ,defillama_stablecoin='optimism'
        ,l2beat_tvl_naming="optimism"
        )
    ,AdapterMapping(
        origin_key='arbitrum'
        ,name = "Arbitrum One"
        ,symbol = "ARB"
        ,technology = "Optimistic Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2021-08-31'
        ,website='https://arbitrum.io/'
        ,block_explorer='https://arbiscan.io/'
        ,twitter='https://twitter.com/arbitrum'

        ,coingecko_naming="arbitrum"
        ,defillama_stablecoin='arbitrum'
        ,l2beat_tvl_naming='arbitrum'
        )
    ,AdapterMapping(
        origin_key="imx"
        ,name = "Immutable X"
        ,symbol = "IMX"
        ,technology = "Validium"
        ,purpose = 'Gaming, NFTs'
        ,launch_date='2021-03-26'
        ,website='https://www.immutable.com/'
        ,block_explorer='https://immutascan.io/'
        ,twitter='https://twitter.com/immutableX'

        ,coingecko_naming="immutable-x"
        #,defillama_stablecoin=''  ## stables via Flipside
        ,l2beat_tvl_naming='immutablex'
        )
    # ,AdapterMapping(
    #     origin_key="loopring"
    #     ,coingecko_naming="loopring"
    #     ,defillama_stablecoin='loopring'
    #     ,l2beat_tvl_naming='loopring'
    #     )
    # ,AdapterMapping(
    #     origin_key='zksync_era'
    #     ,defillama_stablecoin='' ## stables via Flipside
    #     ,l2beat_tvl_naming='zksync-era'
    #     )
    # ,AdapterMapping(
    #     origin_key='zksync_lite'
    #     ,defillama_stablecoin='zksync'
    #     ,l2beat_tvl_naming='zksync-lite'
    #     )

    # ,AdapterMapping(
    #     origin_key='starknet'
    #     ,defillama_stablecoin='starknet'
    #     ,l2beat_tvl_naming='starknet'
    #     )
    # ,AdapterMapping(
    #     origin_key='aztec_v2'
    #     ,defillama_stablecoin='aztec'
    #     ,l2beat_tvl_naming='aztec'
    #     )

] # end of adapter_mappings
