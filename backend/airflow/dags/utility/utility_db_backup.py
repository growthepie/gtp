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
    dag_id='utility_db_backup',
    description='Backup crucial data tables from db.',
    tags=['utility', 'daily'],
    start_date=datetime(2023,7,4),
    schedule='30 04 * * *'
)        

def backup():
    @task()
    def run_backup_tables():
        import os
        import polars as pl
        from src.misc.helper_functions import upload_polars_df_to_s3
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        tables = ['fact_kpis', 'sys_chains', 'sys_rpc_config', 'oli_tag_mapping', 'oli_oss_directory']
        #tables = ['fact_kpis', 'sys_chains', 'sys_rpc_config', 'oli_oss_directory']
        time_str = datetime.now().isoformat()[:10]
        bucket_name = os.getenv("S3_LONG_TERM_BUCKET")

        for table_name in tables:
            print(f'...loading {table_name}')
            if table_name == 'oli_tag_mapping':
                exec_string = f"""select * from oli_tag_mapping where added_on >= current_date - interval '90 days' """
            else:
                exec_string = f'select * from {table_name}'

            df = pl.read_database_uri(query=exec_string, uri=db_connector.uri)

            print(f"...loaded {df.shape[0]} rows.")

            filename = f"{table_name}_{time_str}.parquet"
            file_key = f"backup_db/{table_name}/{filename}"

            upload_polars_df_to_s3(df, filename, bucket_name, file_key)            

            print(f'...finished backing up {table_name}')

    run_backup_tables()
backup()





