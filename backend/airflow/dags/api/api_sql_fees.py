from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook

import pandas as pd
import os
from src.adapters.adapter_sql import AdapterSQL
from src.api.json_creation import JSONCreation
from src.chain_config import adapter_mapping

def create_aggregate_metrics_task(origin_key):
    @task(task_id=f'agg_fees_metrics_{origin_key}')
    def run_aggregate_metrics():
        adapter_params = {}

        load_params = {
            'load_type': 'fees',
            'days': 1,
            'origin_keys': [origin_key],
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)
    
    return run_aggregate_metrics

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_sql_fees',
    description='Run some sql aggregations for fees page.',
    tags=['metrics', 'near-real-time'],
    start_date=datetime(2023,4,24),
    schedule_interval='10,40 * * * *'
)
def fees_json_gen_dag():
        @task()
        def run_create_fees_json():
                db_connector = DbConnector()
                json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, "v1")
                df = json_creator.get_data_fees()
                json_creator.create_fees_table_json(df)
                json_creator.create_fees_linechart_json(df)

        aggregate_metrics_tasks = [
            create_aggregate_metrics_task(chain.origin_key)()
            for chain in adapter_mapping if chain.in_fees_api
        ]
   
        run_create_fees_json().set_upstream(aggregate_metrics_tasks)

fees_json_gen_dag()