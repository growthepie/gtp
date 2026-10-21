import pandas as pd
from datetime import datetime, timezone

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import api_get_call
from src.misc.helper_functions import print_init, print_load, print_extract
from src.main_config import get_main_config
from eim.funcs import get_eim_yamls

## ETH-equivalent tokens as DefiLlama token symbols (uppercase). Base list comes from the EIM eth_derivatives yaml, extended by
## LSTs that are missing there. LRTs are restaked ETH (issued by Liquid Restaking protocols, not part of the liquid staking total).
NATIVE_TOKENS = {'ETH', 'WETH'}
LRT_TOKENS = {'EZETH', 'RSETH', 'RSWETH', 'PUFETH'}
EXTRA_LST_TOKENS = {'WBETH', 'OSETH', 'LSETH', 'ETHX', 'SWETH'}

## only protocols with at least this much TVL on the selected chains are fetched (each /protocol call returns the full history)
MIN_PROTOCOL_TVL_USD = 5e6

class AdapterEthCollateral(AbstractAdapter):
    """
    ETH used as collateral in lending, liquid staking and DEXs on Ethereum + L2s with ETH as gas token, based on DefiLlama.
    Partitioned without overlap:
        lending: all ETH-equivalent tokens (ETH, WETH, LSTs, LRTs) in Lending protocols (DefiLlama TVL, i.e. net of borrows)
        dex: all ETH-equivalent tokens in DEX pools
        liquid_staking: ETH staked via Liquid Staking protocols minus the LST tokens that are deployed in lending / DEXs
    Amounts are converted from USD to ETH with the latest hourly ETH price.

    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("ETH collateral", adapter_params, db_connector)
        self.base_url = 'https://api.llama.fi'
        derivatives = get_eim_yamls(['eth_derivatives'])[0]
        self.lst_tokens = ({k.upper() for k in derivatives.keys()} | EXTRA_LST_TOKENS) - NATIVE_TOKENS - LRT_TOKENS - {'NATIVESTAKEDETH'}
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        none
    """
    def extract(self, load_params:dict):
        chains = self.get_defillama_chains()
        protocols = api_get_call(f'{self.base_url}/protocols', sleeper=10, retries=5)

        lending = self.get_eth_tokens_usd(protocols, 'Lending', chains)
        dex = self.get_eth_tokens_usd(protocols, 'Dexs', chains)
        ## liquid staking happens on Ethereum mainnet
        lst_total_usd = sum((p.get('chainTvls') or {}).get('Ethereum', 0) for p in protocols if p.get('category') == 'Liquid Staking')
        liquid_staking_usd = max(lst_total_usd - lending['lst'] - dex['lst'], 0)

        price = self.db_connector.get_last_price_usd('ethereum', granularity='hourly')
        print(f"...{self.name} - lending {lending}, dex {dex}, liquid staking total {lst_total_usd:,.0f} USD, ETH price {price}")

        values = {
            'collateral_lending_eth': sum(lending.values()) / price,
            'collateral_dex_eth': sum(dex.values()) / price,
            'collateral_liquid_staking_eth': liquid_staking_usd / price,
        }
        df = pd.DataFrame([{'metric_key': k, 'origin_key': 'ethereum', 'date': datetime.now(timezone.utc).date(), 'value': v} for k, v in values.items()])
        df.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        print_extract(self.name, load_params, df.shape)
        return df

    def load(self, df:pd.DataFrame):
        tbl_name = 'eim_fact'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    ## DefiLlama chain names (as used in chainTvls) of Ethereum + L2s with ETH as gas token, mapped via aliases_defillama (slug)
    def get_defillama_chains(self):
        slugs = {c.aliases_defillama for c in get_main_config() if c.metadata_gas_token == 'ETH' and c.api_in_main and c.aliases_defillama}
        llama_chains = api_get_call(f'{self.base_url}/v2/chains', sleeper=10, retries=5)
        names = {c['name'].lower().replace(' ', '-'): c['name'] for c in llama_chains}
        missing = slugs - set(names)
        if missing:
            print(f"...{self.name} - WARNING: no DefiLlama chain found for {missing}")
        return {names[s] for s in slugs if s in names}

    ## USD value of ETH-equivalent tokens in all protocols of a category on the given chains, split by token type
    def get_eth_tokens_usd(self, protocols:list, category:str, chains:set):
        totals = {'native': 0.0, 'lst': 0.0, 'lrt': 0.0}
        candidates = [p for p in protocols if p.get('category') == category and sum((p.get('chainTvls') or {}).get(c, 0) for c in chains) >= MIN_PROTOCOL_TVL_USD]
        print(f"...{self.name} - {len(candidates)} {category} protocols to load")

        for p in candidates:
            response_json = api_get_call(f"{self.base_url}/protocol/{p['slug']}", sleeper=10, retries=3)
            if not response_json:
                print(f"...{self.name} - WARNING: failed to load {p['slug']}, skipping")
                continue
            chain_tvls = response_json.get('chainTvls') or {}
            for chain in chains:
                tokens = (chain_tvls.get(chain) or {}).get('tokensInUsd') or []
                if not tokens:
                    continue
                for symbol, usd in tokens[-1]['tokens'].items():
                    symbol = symbol.upper()
                    if symbol in NATIVE_TOKENS:
                        totals['native'] += usd
                    elif symbol in self.lst_tokens:
                        totals['lst'] += usd
                    elif symbol in LRT_TOKENS:
                        totals['lrt'] += usd
        return totals
