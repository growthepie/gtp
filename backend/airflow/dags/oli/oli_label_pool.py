import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_label_pool',
    description='Loads raw labels into the data pool',
    tags=['contracts', 'oli', 'raw'],
    start_date=datetime(2023, 6, 5),
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)

def main():
    
    @task()
    def sync_attestations():

        from src.db_connector import DbConnector
        from src.adapters.adapter_oli_label_pool import AdapterLabelPool

        db_connector = DbConnector()
        ad = AdapterLabelPool({}, db_connector)

        # get new labels from the GraphQL endpoint
        df = ad.extract()
        
        # load labels into bronze table, then also increment to silver
        ad.load(df)

    sync_attestations()

main()
