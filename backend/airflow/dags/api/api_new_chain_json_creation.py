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
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_new_chain_json_creation',
    description='Create json files for newly listed chain.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule=None,
    params={"api_version": "v1", "origin_key": "ethereum"}
)

def json_creation():
    @task()
    def run_create_jsons(**kwargs):
        import os
        import pickle
        from src.main_config import get_main_config
        from src.da_config import get_da_config
        from src.db_connector import DbConnector
        from src.api.json_creation import JSONCreation
        from src.misc.helper_functions import upload_file_to_cf_s3

        # Get the api_version and origin_key from DAG parameters
        api_version = kwargs['params'].get('api_version', 'v1')
        origin_key = kwargs['params'].get('origin_key', 'ethereum')

        db_connector = DbConnector()

        ## Create new main_conf pickle in S3
        main_conf = get_main_config(db_connector=db_connector, source='github')
        with open("main_conf.pkl", "wb") as file:
            pickle.dump(main_conf, file)
        upload_file_to_cf_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/main_conf.pkl", "main_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))

        ## Create new DA conf pickle in S3
        da_conf = get_da_config(source='github')
        with open("da_conf.pkl", "wb") as file:
            pickle.dump(da_conf, file)
        upload_file_to_cf_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/da_conf.pkl", "da_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))

        ## Create json files
        json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        df = json_creator.get_all_data()
        json_creator.create_master_json(df)
        json_creator.create_landingpage_json(df)
        json_creator.create_chain_details_jsons(df, [origin_key])
        json_creator.create_metric_details_jsons(df)
        json_creator.create_economics_json(df)

    @task()
    def run_create_blockspace_overview(**kwargs):
        import os
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation

        api_version = kwargs['params'].get('api_version', 'v1')

        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_overview_json()

    @task()
    def run_create_blockspace_category_comparison(**kwargs):
        import os
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation

        api_version = kwargs['params'].get('api_version', 'v1')

        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_comparison_json()    

    @task()
    def run_create_chain_blockspace(**kwargs):
        import os
        from src.db_connector import DbConnector
        from src.api.blockspace_json_creation import BlockspaceJSONCreation

        api_version = kwargs['params'].get('api_version', 'v1')
        origin_key = kwargs['params'].get('origin_key', 'ethereum')

        db_connector = DbConnector()
        blockspace_json_creator = BlockspaceJSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)

        blockspace_json_creator.create_blockspace_single_chain_json([origin_key])

    # Main
    run_create_jsons()    
    run_create_blockspace_overview()
    run_create_blockspace_category_comparison()
    run_create_chain_blockspace()
   
json_creation()