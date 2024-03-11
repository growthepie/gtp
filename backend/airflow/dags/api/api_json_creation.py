from datetime import datetime,timedelta
import getpass
import os
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector
from src.api.json_creation import JSONCreation
from src.api.blockspace_json_creation import BlockspaceJSONCreation

api_version = "v1"
db_connector = DbConnector()

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=1),
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
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_chain_details_jsons(df)

    @task()
    def run_create_metrics_details():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_metric_details_jsons(df)

    @task()
    def run_create_landingpage():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_landingpage_json(df)

    @task()
    def run_create_master():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_master_json(df)

    @task()
    def run_create_fundamentals():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_fundamentals_json(df)

    @task()
    def run_create_mvp_dict():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_mvp_dict()

    @task()
    def run_create_contracts():
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_creator.create_contracts_json()

    @task()
    def run_create_blockspace_overview():
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        blockspace_json_creator.create_blockspace_overview_json()

    @task()
    def run_create_blockspace_category_comparison():
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        blockspace_json_creator.create_blockspace_comparison_json()    

    @task()
    def run_create_chain_blockspace():
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        blockspace_json_creator.create_blockspace_single_chain_json()

    run_create_chain_details()
    run_create_metrics_details()
    run_create_landingpage()
    run_create_master()
    run_create_fundamentals()
    run_create_mvp_dict()
    run_create_contracts()
    run_create_blockspace_overview()
    run_create_blockspace_category_comparison()
    run_create_chain_blockspace()
etl()