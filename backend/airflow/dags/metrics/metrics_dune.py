
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
    def run_fact_kpis():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 'auto',
            'load_type' : 'fact_kpis'
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    @task()
    def run_fact_da_consumers():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'query_names' : None,
            'days' : 'auto',
            'load_type' : 'fact_da_consumers'
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
    def check_for_depreciated_L2_trx():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from src.misc.helper_functions import send_discord_message
        
        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'load_type' : 'check-for-depreciated-L2-trx'
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        df = ad.extract(load_params)
        for i, row in df.iterrows():
            send_discord_message(f"<@790276642660548619> The economics mapping function for **{row.l2}** has changed. Details: settlement on {row.settlement_layer}, {row.no_of_trx} trx per day, from_address: {row.from_address}, to_address: {row.to_address}, method: {row.method}.", os.getenv('DISCORD_ALERTS'))
    run_fact_kpis()
    run_fact_da_consumers()
    #run_inscriptions() # paused as of Jan 2025, noone uses inscriptions. Backfilling easily possible if needed
    run_glo_holders()
    check_for_depreciated_L2_trx()
etl()