from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

import os
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector

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
        db_connector = DbConnector()
        tables = ['fact_kpis', 'sys_chains', 'sys_rpc_config', 'oli_tag_mapping', 'oli_oss_directory']
        time_str = datetime.now().isoformat()[:10]
        bucket_name = os.getenv("S3_LONG_TERM_BUCKET")

        for table_name in tables:
            print(f'...loading {table_name}')
            df = db_connector.extract_table(table_name)
            filename = f"{table_name}_{time_str}.parquet"
            file_key = f"backup_db/{table_name}/{filename}"

            s3_path = f"s3://{bucket_name}/{file_key}"
            df.to_parquet(s3_path, index=False)
            print(f'...backed up {len(df)} rows to {file_key}')

    run_backup_tables()
backup()





