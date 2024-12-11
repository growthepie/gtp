
import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_dune',
    description='Load aggregates metrics such as txcount, daa, fees paid, stablecoin mcap where applicable.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,6,5),
    schedule='05 02 * * *'
)

def etl():
    @task()
    def run_aggregates():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 'auto',
            'load_type' : 'metrics'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_inscriptions():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 1000,
            'load_type' : 'inscriptions'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_glo_holders():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'load_type' : 'glo_holders'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def checks_rent_paid_v3_for_heartbeat():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from src.misc.helper_functions import send_discord_message
        
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'load_type' : 'checks-rent-paid-v3'
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        df = ad.extract(load_params)
        for i, row in df.iterrows():
            send_discord_message(f"<@790276642660548619> rent-paid-v3 method depreciated: {row.origin_key}, from_address: {row.from_address}, to_address: {row.to_address}, method: {row.method}", os.getenv('DISCORD_ALERTS'))

    run_aggregates()
    run_inscriptions()
    run_glo_holders()
    checks_rent_paid_v3_for_heartbeat()
etl()