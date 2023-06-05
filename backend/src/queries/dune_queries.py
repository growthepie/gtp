## Dune queries for fundamentals, ethereum_waa, and stables
## The queries always contain a parameter for the number of days to load
## The structure of the Dune output is always the same: day, origin_key, [columns with metric_key as name]
## The metric key columns will be unpivotted into metric_key and value columns

from dune_client.query import Query
from dune_client.types import QueryParameter

dune_queries = [
    Query(name="fundamentals", query_id=2607041, params=[QueryParameter.text_type(name="Days", value="7")]) ## ETH, ARB, OP fundamentals (daily)
    ,Query(name="waa", query_id=2607206, params=[QueryParameter.text_type(name="Days", value="30")]) ## Ethereum weekly active addresses
    ,Query(name="stables_mcap", query_id=2608415, params=[QueryParameter.text_type(name="Days", value="7")]) ## zksync_era, imx, polgyon_zkevm stables_mcap (daily)
]
