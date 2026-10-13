from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Backs up private OLI attestations (incl. raw) as bulk parquet to Hetzner Object Storage and drops
# raw from Postgres once a part is verified. Private attesters are not pinned to IPFS, so the bucket
# is the only copy of their raw afterwards.
# See src/oli/raw_backup.py for layout and completeness checks.
@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_raw_backup',
    description='Daily bulk parquet backup of private OLI attestations to Hetzner Object Storage and drop backed-up raw from Postgres.',
    tags=['oli', 'backup', 'daily'],
    start_date=datetime(2026, 10, 12),
    schedule='30 3 * * *',  # daily 03:30 UTC; backs up all complete UTC days, today's rows are picked up tomorrow
    catchup=False,
    max_active_runs=1
)

def main():

    @task()
    def find_incomplete_days():
        """Compare per-day row count + uid fingerprint in the oli db against the bucket manifests."""
        from src.db_connector import DbConnector
        from src.oli import raw_backup

        db_stats = raw_backup.get_db_day_stats(DbConnector(db_name='oli'))
        manifests = raw_backup.load_manifests(raw_backup.get_s3_client(), raw_backup.get_bucket())
        days = raw_backup.find_incomplete_days(db_stats, manifests)

        print(f"{len(db_stats)} days with private attestations, {len(days)} not fully backed up: {days}")
        return days

    @task()
    def backup_days(days: list):
        """Export every missing attestation of the given days (parts are append-only), then null their raw in Postgres."""
        from datetime import datetime, timezone
        from src.db_connector import DbConnector
        from src.oli import raw_backup

        db_connector = DbConnector(db_name='oli')
        s3 = raw_backup.get_s3_client()
        bucket = raw_backup.get_bucket()
        manifests = raw_backup.load_manifests(s3, bucket)
        run_ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

        missing_raw = {}
        for day in days:
            summary = raw_backup.backup_day(db_connector, s3, bucket, day, manifests.get(day), run_ts)
            print(summary)
            if summary['missing_raw']:
                missing_raw[day] = summary['missing_raw']

        if missing_raw:
            raise RuntimeError(f"Rows without raw and without backup (raw is lost for these): {missing_raw}")

    @task()
    def verify_backup():
        """Fail if any complete UTC day is not fully backed up or a recorded part is missing in the bucket."""
        from src.db_connector import DbConnector
        from src.oli import raw_backup

        s3 = raw_backup.get_s3_client()
        bucket = raw_backup.get_bucket()
        db_stats = raw_backup.get_db_day_stats(DbConnector(db_name='oli'))
        manifests = raw_backup.load_manifests(s3, bucket)

        incomplete = raw_backup.find_incomplete_days(db_stats, manifests)
        broken_parts = raw_backup.verify_parts_exist(s3, bucket, manifests)

        rows = sum(m['rows'] for m in manifests.values())
        size_gb = sum(p['bytes'] for m in manifests.values() for p in m['parts']) / 1e9
        print(f"Backup: {len(manifests)} days, {rows:,} rows, {size_gb:.2f} GB")

        if incomplete or broken_parts:
            raise RuntimeError(f"Incomplete days: {incomplete}; missing/mismatched parts: {broken_parts}")

    backup_days(find_incomplete_days()) >> verify_backup()

main()
