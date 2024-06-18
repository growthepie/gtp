from datetime import datetime,timedelta
import getpass
import os
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook
from src.api.json_creation import JSONCreation
from src.api.blockspace_json_creation import BlockspaceJSONCreation

api_version = "v1"
db_connector = DbConnector()

json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

df = json_creator.get_all_data()

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_json_creation',
    description='Create json files that are necessary to power the frontend.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='30 05 * * *'
)

def etl():
    @task()
    def run_create_chain_details():        
        json_creator.create_chain_details_jsons(df)

    @task()
    def run_create_metrics_details():
        json_creator.create_metric_details_jsons(df)

    @task()
    def run_create_landingpage():
        json_creator.create_landingpage_json(df)

    @task()
    def run_create_economics():
        json_creator.create_economics_json(df)

    @task()
    def run_create_master():
        json_creator.create_master_json(df)

    @task()
    def run_create_fundamentals():
        json_creator.create_fundamentals_json(df)
        json_creator.create_fundamentals_full_json(df)

    @task()
    def run_create_labels():
        json_creator.create_labels_json('full')
        json_creator.create_labels_json('quick')
        json_creator.create_labels_sparkline_json()

        json_creator.create_labels_parquet('full')
        json_creator.create_labels_parquet('quick')
        json_creator.create_labels_sparkline_parquet()
        json_creator.create_projects_parquet()

    @task()
    def run_create_blockspace_overview():
        blockspace_json_creator.create_blockspace_overview_json()

    @task()
    def run_create_blockspace_category_comparison():
        blockspace_json_creator.create_blockspace_comparison_json()    

    @task()
    def run_create_chain_blockspace():
        blockspace_json_creator.create_blockspace_single_chain_json()

    @task()
    def run_create_glo():
        json_creator.create_glo_json()

    # Main
    run_create_master()    
    run_create_chain_details()
    run_create_metrics_details()
    run_create_landingpage()
    run_create_economics()

    ## Blockspace
    run_create_blockspace_overview()
    run_create_blockspace_category_comparison()
    run_create_chain_blockspace()

    ## Labels
    run_create_labels()

    ## Misc
    run_create_glo()
    run_create_fundamentals()
etl()