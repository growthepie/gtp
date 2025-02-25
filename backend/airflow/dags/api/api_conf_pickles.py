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
    dag_id='api_conf_pickles',
    description='Create main and DA conf pickle file.',
    tags=['api', 'daily'],
    start_date=datetime(2024,12,17),
    schedule='30 00 * * *'
)

def json_creation():
    @task()
    def run_create_main_conf():
        import dotenv
        dotenv.load_dotenv()

        import os
        import pickle
        from src.main_config import get_main_config
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_file_to_cf_s3

        db_connector = DbConnector()
        api_version = "v1"

        ## Upload new main_conf to S3
        main_conf = get_main_config(db_connector=db_connector, source='github')
        with open("main_conf.pkl", "wb") as file:
            pickle.dump(main_conf, file)

        upload_file_to_cf_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/main_conf.pkl", "main_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))

    @task()
    def run_create_da_conf():
        import dotenv
        dotenv.load_dotenv()
        
        import os
        import pickle
        from src.da_config import get_da_config
        from src.misc.helper_functions import upload_file_to_cf_s3

        api_version = "v1"

        ## Upload new main_conf to S3
        da_conf = get_da_config(source='github')
        with open("da_conf.pkl", "wb") as file:
            pickle.dump(da_conf, file)

        upload_file_to_cf_s3(os.getenv("S3_CF_BUCKET"), f"{api_version}/da_conf.pkl", "da_conf.pkl", os.getenv("CF_DISTRIBUTION_ID"))

    # Main
    run_create_main_conf()    
    run_create_da_conf()
   
json_creation()