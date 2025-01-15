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
    def run_economics_mapping():
        import requests
        import yaml
        import pandas as pd
        from src.db_connector import DbConnector

        # URL of the raw file from GitHub
        url = "https://raw.githubusercontent.com/growthepie/gtp-dna/refs/heads/main/economics_da/economics_mapping.yml"
        response = requests.get(url)

        data = yaml.load(response.text, Loader=yaml.FullLoader)
        table = [
            [
                L2,
                layers.get('name'), 
                layers.get('gtp_origin_key'),
                settlement_layer, 
                f.get('from_address'), 
                f.get('to_address'), 
                f.get('method'), 
                f.get('namespace') if settlement_layer == 'celestia' else None
            ]
            for L2, layers in data.items()
            for settlement_layer, filters in layers.items() if isinstance(filters, list)
            for f in filters
        ]
        df = pd.DataFrame(table, columns=['da_consumer_key', 'name', 'gtp_origin_key', 'da_layer', 'from_address', 'to_address', 'method', 'namespace'])

        ## in column da_layer rename 'celestia' to 'da_celestia', 'L1' to 'da_ethereum_calldata', 'beacon' to 'da_ethereum_blobs'
        df['da_layer'] = df['da_layer'].replace({'celestia': 'da_celestia', 'L1': 'da_ethereum_calldata', 'beacon': 'da_ethereum_blobs'})

        df.set_index(['da_consumer_key', 'da_layer', 'from_address', 'to_address', 'method', 'namespace'], inplace=True)

        db_connector = DbConnector()
        db_connector.upsert_table('sys_economics_mapping', df)


    run_unique_senders()
    run_da_queries()
    run_economics_mapping()
etl()





