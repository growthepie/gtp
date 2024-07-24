from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from src.misc.octant import Octant

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 10,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_octant',
    description='Load data from Octant API for tracker.',
    tags=['other', 'near-real-time'],
    start_date=datetime(2024,7,22),
    schedule='*/2 * * * *'
)

def run_dag():
    @task()
    def run_data_export():
        db_connector = DbConnector(db_name='fun')

        oct = Octant()
        last_epoch = oct.get_last_epoch()
        start_end_times = oct.get_epoch_start_end_times(last_epoch)
        epoch_data, checksum = oct.get_epochs_data(start_end_times)

        exec_string = """
            SELECT checksum
            FROM octant_rounds
            ORDER BY id DESC
            LIMIT 1;
        """
        db_checksum = db_connector.engine.execute(exec_string).scalar()

        if db_checksum == checksum:
            print('No new data')
        else:
            print('New data, inserting into db')   
            exec_string = f"""
                INSERT INTO octant_rounds (value, checksum, created_at)
                VALUES ('{epoch_data}', '{checksum}', NOW());
            """
            db_connector.engine.execute(exec_string)
    run_data_export()
run_dag()