from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector

import pandas as pd
from src.chain_config import adapter_mapping
from src.api.json_creation import JSONCreation

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='api_sql_fees',
    description='Run some sql aggregations for fees page.',
    tags=['metrics', 'near-real-time'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/15 * * * *'
)

def etl():
        @task()
        def run_aggregate_metrics():
                db_connector = DbConnector()
                for chain in adapter_mapping:
                        origin_key = chain.origin_key
                        if chain.in_fees_api == False:
                                print(f"...skipping {origin_key} as it is not in the fees api")
                        else:
                                print(f"... processing {origin_key}")
                                ## txcosts_average
                                exec_string = f"""
                                        SELECT
                                                date_trunc('hour', "block_timestamp") AS timestamp,
                                                '{origin_key}' as origin_key,
                                                'txcosts_avg_eth' as metric_key,
                                                'hourly' as granularity,
                                                AVG(tx_fee) as value
                                        FROM public.{origin_key}_tx
                                        WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '3 days' AND now()
                                        AND empty_input = TRUE
                                        GROUP BY 1,2,3,4
                                """

                                df = pd.read_sql(exec_string, db_connector.engine.connect())
                                df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                db_connector.upsert_table('fact_kpis_granular', df)

                                ## txcosts_median
                                exec_string = f"""
                                        WITH 
                                        median_tx AS (
                                                SELECT
                                                        date_trunc('hour', "block_timestamp") AS timestamp,
                                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                                                FROM public.{origin_key}_tx
                                                WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '3 days' AND now()
                                                GROUP BY 1
                                        )

                                        SELECT
                                                '{origin_key}' as origin_key,
                                                'txcosts_median_eth' as metric_key,
                                                z.timestamp,
                                                'hourly' as granularity,
                                                z.median_tx_fee as value
                                        FROM median_tx z
                                """

                                df = pd.read_sql(exec_string, db_connector.engine.connect())
                                df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                db_connector.upsert_table('fact_kpis_granular', df)

                                ## txcosts_native_median
                                exec_string = f"""
                                        WITH 
                                        median_tx AS (
                                                SELECT
                                                        date_trunc('hour', "block_timestamp") AS timestamp,
                                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                                                FROM public.{origin_key}_tx
                                                WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '3 days' AND now()
                                                        AND empty_input = TRUE
                                                GROUP BY 1
                                        )

                                        SELECT
                                                '{origin_key}' as origin_key,
                                                'txcosts_native_median_eth' as metric_key,
                                                z.timestamp,
                                                'hourly' as granularity,
                                                z.median_tx_fee as value
                                        FROM median_tx z
                                """

                                df = pd.read_sql(exec_string, db_connector.engine.connect())
                                df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                db_connector.upsert_table('fact_kpis_granular', df)

        @task()
        def run_create_fees_json():
                json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
                json_creator.create_fees_json()
   
        run_create_fees_json(run_aggregate_metrics())
etl()