from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
    },
    dag_id='metrics_stables_balances',
    description='Stablecoin balances of tracked addresses via balanceOf (Multicall3)',
    tags=['metrics', 'daily'],
    start_date=datetime(2026,10,7),
    schedule='45 01 * * *' # after utility_first_block (00:10) and metrics_stables_v2 (01:15)
)

def etl():

    @task()
    def run_stables_balances():
        from src.db_connector import DbConnector
        from src.adapters.adapter_stables_balances import AdapterStablecoinBalances

        db_connector = DbConnector()
        ad = AdapterStablecoinBalances({}, db_connector)

        # trial scope: 3 chains; discovery = most used addresses + OLI labeled apps + Dune top receivers of the last 7 days (daily)
        # + Blockscout current top holders (first run + Mondays; needs BLOCKSCOUT_API_KEY_PRO, Polygon needs a higher plan)
        # 'dune_history' (yearly windows) is a manual one-time seed for chains without Blockscout access
        ad.extract({
            'origin_keys': ['ethereum', 'polygon_pos', 'celo'], # target: ['*'] = all app metrics chains (minus starknet)
            'days': 21,
            'sources': ['activity', 'oli', 'dune', 'blockscout'],
            'top_n': 1000,
            'dune_top_n': 1000,
            'dune_days': 7,
            'dune_min_received_usd': 100_000,
            'blockscout_top_n': 1000,
            'blockscout_min_usd': 10_000,
            'blockscout_weekday': 0,
            'prune_min_usd': 10_000,          # stop measuring candidates without OLI owner that stay below this
        })

    run_stables_balances()

etl()
