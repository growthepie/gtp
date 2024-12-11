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
    dag_id='utility_sql_materialize',
    description='Aggregate materialized views on database',
    tags=['utility', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='00 02 * * *'
)

def etl():
    @task()
    def run_unique_senders():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        adapter_params = {}
        load_params = {
            'load_type' : 'active_addresses_agg',
            'days' : 3, ## days as int or 'auto
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)

    @task()
    def run_da_queries():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        adapter_params = {}
        load_params = {
            'load_type' : 'jinja', ## usd_to_eth or metrics or blockspace
            'queries' : ['da_metrics/upsert_fact_da_consumers_celestia_blob_size.sql.j2', 'da_metrics/upsert_fact_da_consumers_celestia_blob_fees.sql.j2'],
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)

        # extract
        ad.extract(load_params)

    @task()
    def run_da_mapping():
        import yaml
        import pandas as pd
        from src.db_connector import DbConnector

        ## read file src/da_mapping.yml into dict
        if sys_user == 'ubuntu':
                with open(f'/home/{sys_user}/gtp/backend/src/da_mapping.yml', 'r') as file:
                    da_mapping = yaml.safe_load(file)
        else:
                with open('src/da_mapping.yml', 'r') as file:
                    da_mapping = yaml.safe_load(file)
            
        # Flatten the nested dictionary and extract relevant fields
        rows = []
        for da_layer, da_keys in da_mapping.items():
            for da_key, details in da_keys.items():
                rows.append({
                    'da_layer': da_layer,
                    'da_consumer_key': da_key,
                    'gtp_origin_key': details.get('gtp_origin_key', None),
                    'name': details.get('name', None),
                    'namespace': details.get('namespace', None),
                })

        # Create the DataFrame
        df = pd.DataFrame(rows)
        df.set_index(['da_layer', 'da_consumer_key'], inplace=True)

        db_connector = DbConnector()
        db_connector.upsert_table('sys_da_mapping', df)

    run_unique_senders()
    run_da_queries()
    run_da_mapping()
etl()





