import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from jinja2 import Environment, FileSystemLoader, StrictUndefined
if sys_user == 'ubuntu':
        env = Environment(loader=FileSystemLoader(f'/home/{sys_user}/gtp/backend/src/queries/postgres'), undefined=StrictUndefined)
else:
        env = Environment(loader=FileSystemLoader('src/queries/postgres'), undefined=StrictUndefined)

class SQLQuery():
    def __init__(self, jinja_path: str, metric_key: str, origin_key: str, query_parameters: dict = None, currency_dependent:bool = True):
        self.template = env.get_template(jinja_path)
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters if query_parameters is not None else {}
        self.query_parameters['origin_key'] = origin_key
        self.currency_dependent = currency_dependent ## if false, the query can in parellel to the currency queries 

def standard_evm_queries(origin_key: str): ## op-stack and others
     return [
                SQLQuery(metric_key = "txcount_raw", origin_key = origin_key, jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
                ,SQLQuery(metric_key = "txcount", origin_key = origin_key, jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
                ,SQLQuery(metric_key = "daa", origin_key = origin_key, jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "maa", origin_key = origin_key, jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "aa_last7d", origin_key = origin_key, jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
                ,SQLQuery(metric_key = "aa_last30d", origin_key = origin_key, jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
                ,SQLQuery(metric_key = "gas_per_second", origin_key = origin_key, jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = origin_key, jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = origin_key, jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "fees_paid_eth", origin_key = origin_key, jinja_path='chain_metrics/select_fees_paid.sql.j2')
                ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = origin_key, jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ]

## Queries have default values defined in the jinja templates. These can either be overwritten here or later in adater_sql.py
sql_queries = [
        # Multi-chain
        SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", jinja_path='chain_metrics/select_user_base_weekly.sql.j2', currency_dependent = False)
        
        # --- Layer 1 ---
        # Ethereum
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "ethereum", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "ethereum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "ethereum", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "ethereum", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        
        # --- DA Layers ---
        # Celestia
        ,SQLQuery(metric_key = "da_data_posted_bytes", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_data_posted_bytes.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_unique_blob_producers", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_unique_blob_producers.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_blob_count", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_blob_count.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_fees_eth", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_fees_eth.sql.j2')

        # --- Layer 2s ---
        # Arbitrum Orbit
        # Different filter col on txcounts (gas_used instead of gas_price)
        ## Arbitrum (some data pulled via Dune)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "arbitrum", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "arbitrum", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Real
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "real", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "real", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "real", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "real", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "real", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "real", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "real", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "real", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "real", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "real", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "real", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        ## Arbitrum Nova
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        # SUPERCHAIN
        ## OP Mainnet
        ,*standard_evm_queries("optimism")
        ## Base
        ,*standard_evm_queries("base")
        ## Zora
        ,*standard_evm_queries("zora")        
        ## Mode
        ,*standard_evm_queries("mode")
        ## Redstone
        ,*standard_evm_queries("redstone")
        ## Derive
        ,*standard_evm_queries("derive")
        ## Orderly
        ,*standard_evm_queries("orderly")
        ## Worldchain
        ,*standard_evm_queries("worldchain")
        ## Mint
        ,*standard_evm_queries("mint")
        ## Fraxtal
        ,*standard_evm_queries("fraxtal")
        ## INK
        ,*standard_evm_queries("ink")
        ## Soneium
        ,*standard_evm_queries("soneium")

        ## Swell
        ,*standard_evm_queries("swell")

        # Elastic Chain
        ## ZKsync Era
        ,*standard_evm_queries("zksync_era")

        # Polygon zkStack
        ## Polygon zkEVM
        ,*standard_evm_queries("polygon_zkevm")

        # Others EVM
        ## Linea
        ,*standard_evm_queries("linea")
        ## Scroll
        ,*standard_evm_queries("scroll")
        ## Blast
        ,*standard_evm_queries("blast")
        ## Taiko
        ,*standard_evm_queries("taiko")
        ## Manta
        ,*standard_evm_queries("manta")
        ## Zircuit
        ,*standard_evm_queries("zircuit")
        ## Unichain
        ,*standard_evm_queries("unichain")

        # Others EVM Custom Gas Token
        ## Mantle (also custom gas query)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "mantle", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "mantle", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mantle", jinja_path='chain_metrics/custom/mantle_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "mantle", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "mantle", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        ## Metis
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "metis", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "metis", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "metis", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "metis", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "metis", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "metis", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "metis", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        ## Gravity (Orbit tx filter, also custom gas query)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "gravity", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "gravity", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "gravity", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "gravity", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "gravity", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gravity", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gravity", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        
        ## Celo (op stack but CELO gas token and only data available start March 26th when L2 went live)
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "celo", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "celo", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "celo", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "celo", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "celo", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "celo", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "celo", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "celo", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "celo", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "celo", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')

        # Others Non-EVM
        ## IMX
        ,SQLQuery(metric_key = "txcount", origin_key = "imx", jinja_path='chain_metrics/custom/imx_select_txcount.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "imx", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "imx", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "imx", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "imx", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "imx", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "imx", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Loopring
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "loopring", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "loopring", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "loopring", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "loopring", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        # ## Rhino
        # ,SQLQuery(metric_key = "txcount_raw", origin_key = "rhino", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount", origin_key = "rhino", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "daa", origin_key = "rhino", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "maa", origin_key = "rhino", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last7d", origin_key = "rhino", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last30d", origin_key = "rhino", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "rhino", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "rhino", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Starknet
        ,SQLQuery(metric_key = "txcount_raw", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "starknet", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "starknet", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "user_base_weekly", origin_key = "starknet", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "starknet", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "starknet", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "starknet", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
]