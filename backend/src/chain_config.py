from pydantic import BaseModel
from typing import Optional

## Adapter Mappings
## WARNING: When a new variable is added (which is not optional), it also needs to be added to the adapter_multi_mapping at the end of this file
class AdapterMapping(BaseModel):
    origin_key: str
    name: str 
    name_short: str ##max 10 characters
    in_api: bool ## True when the chain should be included in the API output
    exclude_metrics: list[str] ## list of metrics to exclude from the API output. Either metric name or "blockspace"
    aggregate_blockspace: bool ## True when the chain should be included in the blockspace aggregation

    bucket: str ## for Menu (and potentially filters): Layer 1, OP Chains, Other Optimistic Rollups, ZK-Rollups, Offchain Data Availability 
    technology: str ## -, zk, optimistic
    purpose: str ## is it a general purpose chain, or a specialized one?
    symbol: Optional[str]
    launch_date: Optional[str] ## YYYY-MM-DD
    website: Optional[str]
    block_explorer: Optional[str]
    twitter: Optional[str]

    coingecko_naming: Optional[str] ## to load price, market cap, etc
    l2beat_tvl_naming: Optional[str] ## to load tvl
    defillama_stablecoin : Optional[str] ## to load stablecoin tvl (if commented out, stables are loaded via Dune Query)

    ## for txcount cross-check with block explorers
    block_explorer_txcount: Optional[str]
    block_explorer_type: Optional[str] ## 'etherscan' or 'blockscout'

adapter_mapping = [
    # Layer 1
    AdapterMapping(
        origin_key="ethereum"
        ,name = "Ethereum"
        ,name_short = "Ethereum"
        ,in_api = True
        ,exclude_metrics = ['tvl', 'rent_paid', 'profit', 'blockspace']
        ,aggregate_blockspace = False

        ,bucket = "Layer 1"
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
        ,name_short = "Polygon"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "ZK-Rollups"
        ,symbol = "MATIC"
        ,technology = "ZK Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-03-24'
        ,website='https://polygon.technology/polygon-zkevm'
        ,block_explorer='https://zkevm.polygonscan.com/'
        ,twitter='https://twitter.com/0xPolygon'

        ,coingecko_naming="matic-network"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='polygonzkevm'

        ,block_explorer_txcount='https://zkevm.polygonscan.com/chart/tx?output=csv'
        ,block_explorer_type='etherscan' 
        )

    ,AdapterMapping(
        origin_key="optimism"
        ,name = "Optimism"
        ,name_short = "Optimism"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "OP Chains"
        ,symbol = "OP"
        ,technology = "Optimistic Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2021-12-16'
        ,website='https://optimism.io/'
        ,block_explorer='https://optimistic.etherscan.io/'
        ,twitter='https://twitter.com/Optimism'

        ,coingecko_naming="optimism"
        ,defillama_stablecoin='optimism'
        ,l2beat_tvl_naming="optimism"

        # ,block_explorer_txcount='https://optimistic.etherscan.io/chart/tx?output=csv' 
        # ,block_explorer_type='etherscan'
        ,block_explorer_txcount="https://l2beat.com/api/activity/optimism.json"
        ,block_explorer_type='l2beat'
        )

    ,AdapterMapping(
        origin_key='arbitrum'
        ,name = "Arbitrum One"
        ,name_short = "Arbitrum"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "Other Optimistic Rollups"
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

        ,block_explorer_txcount='https://arbiscan.io/chart/tx?output=csv'
        ,block_explorer_type='etherscan'
        )

    ,AdapterMapping(
        origin_key="imx"
        ,name = "Immutable X"
        ,name_short = "IMX"
        ,in_api = True
        ,exclude_metrics = ['txcosts', 'fees', 'profit']
        ,aggregate_blockspace = True

        ,bucket = "Offchain Data Availability"
        ,symbol = "IMX"
        ,technology = "Validium"
        ,purpose = 'Gaming, NFTs'
        ,launch_date='2021-03-26'
        ,website='https://www.immutable.com/'
        ,block_explorer='https://immutascan.io/'
        ,twitter='https://twitter.com/immutable'

        ,coingecko_naming="immutable-x"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='immutablex'
        )

    ,AdapterMapping(
        origin_key="zksync_era"
        ,name = "zkSync Era"
        ,name_short = "zkSync Era"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "ZK-Rollups"
        ,symbol = "-"
        ,technology = "ZK Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-03-24'
        ,website='https://zksync.io/'
        ,block_explorer='https://explorer.zksync.io/'
        ,twitter='https://twitter.com/zksync'

        #,coingecko_naming="-"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='zksync-era'

        ,block_explorer_txcount="https://l2beat.com/api/activity/zksync-era.json"
        ,block_explorer_type='l2beat'
    )

    ,AdapterMapping(
        origin_key="base"
        ,name = "Base"
        ,name_short = "Base"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "OP Chains"
        ,symbol = "-"
        ,technology = "Optimistic Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-07-13'
        ,website='https://base.org/'
        ,block_explorer='https://basescan.org/'
        ,twitter='https://twitter.com/base'

        #,coingecko_naming="-"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='base'

        ,block_explorer_txcount='https://basescan.org/chart/tx?output=csv'
        ,block_explorer_type='etherscan'
    )

    ,AdapterMapping(
        origin_key="zora"
        ,name = "Zora"
        ,name_short = "Zora"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "OP Chains"
        ,symbol = "-"
        ,technology = "Optimistic Rollup"
        ,purpose = 'NFTs'
        ,launch_date='2023-06-21'
        ,website='https://zora.co/'
        ,block_explorer='https://explorer.zora.energy/'
        ,twitter='https://twitter.com/ourzora'

        #,coingecko_naming="-"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='zora'

        ,block_explorer_txcount='https://explorer.zora.energy/api/v2/stats/charts/transactions'
        ,block_explorer_type='blockscout'
    )

    ,AdapterMapping(
        origin_key="gitcoin_pgn"
        ,name="Public Goods Network"
        ,name_short = "PGN"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "Offchain Data Availability"
        ,symbol = "-"
        ,technology = "Optimium"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-07-26'
        ,website='https://publicgoods.network/'
        ,block_explorer='https://explorer.publicgoods.network/'
        ,twitter="https://twitter.com/pgn_eth"

        #,coingecko_naming="-"
        #,defillama_stablecoin=''  ## stables via Dune
        ,l2beat_tvl_naming='publicgoodsnetwork'

        ,block_explorer_txcount='https://explorer.publicgoods.network/api/v2/stats/charts/transactions'
        ,block_explorer_type='blockscout'
    )

    ,AdapterMapping(
        origin_key="linea"
        ,name="Linea"
        ,name_short = "Linea"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "ZK-Rollups"
        ,symbol = "-"
        ,technology = "ZK Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-07-12'
        ,website='https://linea.build/'
        ,block_explorer='https://lineascan.build/'
        ,twitter="https://twitter.com/LineaBuild"

        #,coingecko_naming="linea"
        #,defillama_stablecoin='Linea' ## stables via Dune
        ,l2beat_tvl_naming='linea'

        ,block_explorer_txcount='https://lineascan.build/chart/tx?output=csv'
        ,block_explorer_type='etherscan'
    )

    ,AdapterMapping(
        origin_key='scroll'
        ,name='Scroll'
        ,name_short = "Scroll"
        ,in_api = True
        ,exclude_metrics = []
        ,aggregate_blockspace = True

        ,bucket = "ZK-Rollups"
        ,symbol = "-"
        ,technology = "ZK Rollup"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-10-17'
        ,website='https://scroll.io/'
        ,block_explorer='https://scrollscan.com/'
        ,twitter="https://twitter.com/scroll_zkp"

        #,coingecko_naming="scroll"
        #,defillama_stablecoin='' ## stables via Dune
        ,l2beat_tvl_naming='scroll'

        ,block_explorer_txcount='https://scrollscan.com/chart/tx?output=csv'
        ,block_explorer_type='etherscan'
    )

    ,AdapterMapping(
        origin_key='mantle'
        ,name='Mantle'
        ,name_short = "Mantle"
        ,in_api = True
        ,exclude_metrics = ['profit']
        ,aggregate_blockspace = True

        ,bucket = "Offchain Data Availability"
        ,symbol = "-"
        ,technology = "Optimium"
        ,purpose = 'General Purpose (EVM)'
        ,launch_date='2023-07-14'
        ,website='https://mantle.xyz/'
        ,block_explorer='https://explorer.mantle.xyz/'
        ,twitter="https://twitter.com/0xMantle"

        ,coingecko_naming="mantle"
        # ,defillama_stablecoin='Mantle' ## stables via Dune
        ,l2beat_tvl_naming='mantle'

        ,block_explorer_txcount="https://l2beat.com/api/activity/mantle.json"
        ,block_explorer_type='l2beat'
    )


    ,AdapterMapping(
        origin_key="loopring"
        ,name="Loopring"
        ,name_short = "Loopring"
        ,in_api = True
        ,exclude_metrics = ['blockspace', 'profit', 'fees', 'txcosts']
        ,aggregate_blockspace = False

        ,bucket = "ZK-Rollups"
        ,symbol = "LRC"
        ,technology = "ZK Rollup"
        ,purpose = 'Token Transfers, NFTs, Swaps'
        ,launch_date='2019-12-04'
        ,website='https://loopring.org/'
        ,block_explorer='https://explorer.loopring.io/'
        ,twitter='https://twitter.com/loopringorg'

        ,coingecko_naming="loopring"
        ,defillama_stablecoin='Loopring'
        ,l2beat_tvl_naming='loopring'

        ,block_explorer_txcount="https://l2beat.com/api/activity/loopring.json"
        ,block_explorer_type='l2beat'
        )

    # ,AdapterMapping(
    #     origin_key='starknet'
    #     ,defillama_stablecoin='starknet'
    #     ,l2beat_tvl_naming='starknet'
    #     )

] # end of adapter_mappings

adapter_all2_mapping = adapter_mapping + [AdapterMapping(origin_key='all_l2s', name='All L2s', in_api=True, exclude_metrics=[], aggregate_blockspace=False, technology='-', purpose='-', name_short='-', bucket='-')] ## for multi-chain metrics
adapter_multi_mapping = adapter_all2_mapping + [AdapterMapping(origin_key='multiple', name='Multiple L2s', in_api=True, exclude_metrics=[], aggregate_blockspace=False, technology='-', purpose = '-', name_short='-', bucket='-')]
