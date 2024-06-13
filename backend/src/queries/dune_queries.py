## Dune queries for fundamentals, ethereum_waa, and stables
## The queries always contain a parameter for the number of days to load
## The structure of the Dune output is always the same: day, origin_key, [columns with metric_key as name]
## The metric key columns will be unpivotted into metric_key and value columns

from dune_client.query import QueryBase
from dune_client.types import QueryParameter

dune_queries = [
    QueryBase(name="fundamentals", query_id=2607041, params=[QueryParameter.text_type(name="Days", value="7")]) ## ETH, ARB, OP fundamentals (daily)
    # ,QueryBase(name="waa", query_id=2607206, params=[QueryParameter.text_type(name="Days", value="30")]) ## Ethereum weekly active addresses
    # ,QueryBase(name="maa", query_id=3358664, params=[QueryParameter.text_type(name="Days", value="60")]) ## Ethereum monthly active addresses
    # ,QueryBase(name="aa_last30d", query_id=3359100, params=[]) ## Ethereum active addresses last 30d
    ,QueryBase(name="stables_mcap", query_id=2608415, params=[QueryParameter.text_type(name="Days", value="7")])
    # ,QueryBase(name="stables_mcap", query_id=3808795, params=[QueryParameter.text_type(name="Days", value="7")]) ## zksync_era, imx, polgyon_zkevm stables_mcap (daily)
    ,QueryBase(name="rent_paid", query_id=2986216, params=[QueryParameter.text_type(name="Days", value="7")]) ## Rent paid to mainnet for all L2s
    ,QueryBase(name="inscriptions", query_id=3346613, params=[QueryParameter.text_type(name="Days", value="1000")]) ## Load inscription addresses and store in inscription_addresses table
    ,QueryBase(name="glo_holders", query_id=3732844) ## top 20 glo holders
]