from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

import os

api_version = "v1"

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_eth_asset',
    description='Load ETH asset KPIs (supply, burn, staking) and create the ETH asset page jsons.',
    tags=['api', 'eim', 'hourly'],
    start_date=datetime(2026,10,14),
    schedule='35 * * * *' ## after metrics_coingecko_hourly (hourly ETH price at :20)
)

def etl():
    @task()
    def run_eth_supply_hourly():
        from src.db_connector import DbConnector
        from eim.adapters.adapter_eth_supply import AdapterEthSupply

        db_connector = DbConnector()
        ad = AdapterEthSupply({}, db_connector)
        df = ad.extract({'load_type': 'extract_eth_supply_hourly'})
        ad.load(df)

    @task()
    def run_eth_burned():
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        ## hourly into fact_kpis_granular, daily into eim_fact
        df = db_connector.get_eth_burned('hourly', hours=48)
        upserted = db_connector.upsert_table('fact_kpis_granular', df)
        print(f"Upserted {upserted} hourly eth_burned_eth rows into fact_kpis_granular.")

        df = db_connector.get_eth_burned('daily', hours=24*5)
        upserted = db_connector.upsert_table('eim_fact', df)
        print(f"Upserted {upserted} daily eth_burned_eth rows into eim_fact.")

    @task()
    def run_eth_staking():
        from datetime import datetime, timezone, timedelta
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from eim.adapters.adapter_eth_supply import AdapterEthSupply
        from src.misc.helper_functions import send_discord_message

        db_connector = DbConnector()
        ad = AdapterDune({'api_key': os.getenv("DUNE_API"), 'query_speed': 'medium'}, db_connector)

        ## staked ETH & net CL rewards hourly from ethsupply.fyi (live, actual balance)
        ## fallback for staked ETH: Dune (lags a few hours and is effective balance, ~0.15% lower; json creation forward fills)
        try:
            ad_supply = AdapterEthSupply({}, db_connector)
            df = ad_supply.extract({'load_type': 'extract_eth_staking_hourly', 'range': '24h'})
            ad_supply.load(df)
        except Exception as e:
            print(f"ethsupply.fyi failed ({e}), falling back to Dune for hourly staked ETH.")
            send_discord_message(f"api_eth_asset: ethsupply.fyi staking data failed, used Dune fallback for staked ETH (no hourly CL rewards): {e}")
            df = ad.extract({
                'queries': [{'name': 'eth_staked_hourly', 'query_id': 8951203, 'params': {'hours': 48}}],
                'prepare_df': 'prepare_df_metric_hourly',
                'load_type': 'fact_kpis_granular'
            })
            ad.load(df)

        ## daily net CL rewards & yield from complete days of hourly ethsupply.fyi data
        df = db_connector.get_eth_staking_cl_daily(days=3)
        upserted = db_connector.upsert_table('eim_fact', df)
        print(f"Upserted {upserted} daily CL staking rows into eim_fact.")

        ## staked ETH & EL rewards/yield daily from Dune: only run until yesterday is loaded (Dune ingests the previous day in the early morning UTC)
        ## CL rewards from Dune are not used anymore (unreliable since Pectra), see get_eth_staking_cl_daily
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        last_date = db_connector.execute_query("SELECT MAX(date) AS d FROM eim_fact WHERE metric_key = 'staking_yield_el' AND origin_key = 'ethereum'", load_df=True)['d'].iloc[0]
        if last_date is not None and last_date >= yesterday:
            print(f"Daily EL staking data already loaded until {last_date}, skipping.")
            return

        df = ad.extract({
            'queries': [{'name': 'eth_staking_daily', 'query_id': 8951204, 'params': {'days': 5, 'end_days': 0}}],
            'prepare_df': 'prepare_df_metric_daily',
            'load_type': 'eim_fact'
        })
        if df.empty:
            print("No daily staking data returned from Dune, retrying next run.")
            return
        df = df[df.index.get_level_values('metric_key').isin(['eth_staked_eth', 'staking_rewards_el_eth', 'staking_yield_el', 'staking_rewards_tips_eth', 'staking_rewards_mev_eth', 'staking_yield_tips', 'staking_yield_mev'])]
        ad.load(df)

    @task()
    def run_create_kpis_json():
        from src.db_connector import DbConnector
        from src.api.eth_asset_json import EthAssetJSONCreation

        db_connector = DbConnector()
        json_creator = EthAssetJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_kpis_json()

    @task()
    def run_create_stats_json():
        from src.db_connector import DbConnector
        from src.api.eth_asset_json import EthAssetJSONCreation

        db_connector = DbConnector()
        json_creator = EthAssetJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_stats_json()

    @task()
    def run_eth_collateral():
        from datetime import datetime, timezone
        from src.db_connector import DbConnector
        from eim.adapters.adapter_eth_collateral import AdapterEthCollateral

        db_connector = DbConnector()
        ## daily snapshot (DefiLlama /protocol calls return the full history, ~500MB per run), skip if today is already loaded
        today = datetime.now(timezone.utc).date()
        last_date = db_connector.execute_query("SELECT MAX(date) AS d FROM eim_fact WHERE metric_key = 'collateral_lending_eth' AND origin_key = 'ethereum'", load_df=True)['d'].iloc[0]
        if last_date is not None and last_date >= today:
            print(f"Collateral already loaded for {last_date}, skipping.")
            return

        ad = AdapterEthCollateral({}, db_connector)
        df = ad.extract({})
        ad.load(df)

    @task()
    def run_create_properties_json():
        from src.db_connector import DbConnector
        from src.api.eth_asset_json import EthAssetJSONCreation

        db_connector = DbConnector()
        json_creator = EthAssetJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_properties_json()

    @task()
    def run_create_market_caps_json():
        from src.db_connector import DbConnector
        from src.api.eth_asset_json import EthAssetJSONCreation

        db_connector = DbConnector()
        json_creator = EthAssetJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_market_caps_json()

    ## independent of the other tasks (CoinGecko + Yahoo Finance, nothing stored in the DB)
    run_create_market_caps_json()

    supply = run_eth_supply_hourly()
    staking = run_eth_staking()
    [supply, run_eth_burned(), staking] >> run_create_kpis_json()
    supply >> run_create_stats_json()
    @task()
    def run_reference_data():
        from datetime import datetime, timezone
        from src.db_connector import DbConnector
        from eim.adapters.adapter_reference_data import AdapterReferenceData

        ## once a day is enough (WGC updates the gold stock quarterly)
        if datetime.now(timezone.utc).hour != 6:
            print("Reference data is only refreshed at 06:xx UTC, skipping.")
            return

        db_connector = DbConnector()
        ad = AdapterReferenceData({}, db_connector)
        df = ad.extract({'load_type': 'gold_stock'})
        ad.load(df)

    [supply, staking, run_eth_collateral(), run_reference_data()] >> run_create_properties_json()

etl()
