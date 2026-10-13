"""
Bulk Parquet backups of all offchain OLI attestations to Hetzner Object Storage, after which raw
(the signed envelope, ~70% of public.attestations in the oli db) is dropped from Postgres.

Layout: s3://<HETZNER_BUCKET_NAME>/oli/attestations/date=YYYY-MM-DD/
    part-<run_ts>-<n>.parquet   all attestation columns, one row per uid
    _manifest.json              parts, totals and the DB state the day was last verified against

- Partitioned by the UTC date of attestations."time", which never changes for a row.
- Parts are append-only and never overwritten. Late rows for an already backed-up day get an
  extra part with only the missing uids.
- A day is complete when the DB row count + uid fingerprint match what the manifest was verified
  against (db_rows / db_uid_fingerprint). The fingerprint is an order-independent sum of md5(uid)
  prefixes, computed identically in Postgres (FINGERPRINT_SQL) and Python (uid_fingerprint).
- The current UTC day is never backed up (still growing); it is picked up by the next run.
- Right after a part is verified (read back locally + uploaded size matches), raw is set to NULL in
  Postgres for exactly that part's uids. Rows that are already backed up but still have raw (e.g. an
  interrupted run) get nulled the next time their day is processed.
"""

import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
from time import sleep

import boto3
import psycopg2
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from src.misc.helper_functions import upload_file_to_s3

HETZNER_ENDPOINT = "https://fsn1.your-objectstorage.com"
PREFIX = "oli/attestations"
PART_ROWS = 200_000
FETCH_ROWS = 20_000

# sum() over this is the per-day uid fingerprint; must match uid_fingerprint() below
FINGERPRINT_SQL = "('x' || left(md5(uid), 15))::bit(60)::bigint"

COLUMNS = [
    ("uid", pa.binary()),
    ("time", pa.timestamp("us", tz="UTC")),
    ("chain_id", pa.string()),
    ("attester", pa.binary()),
    ("recipient", pa.string()),
    ("revoked", pa.bool_()),
    ("is_offchain", pa.bool_()),
    ("tx_hash", pa.binary()),
    ("ipfs_hash", pa.string()),
    ("revocation_time", pa.timestamp("us", tz="UTC")),
    ("tags_json", pa.string()),
    ("raw", pa.string()),
    ("last_updated_time", pa.timestamp("us", tz="UTC")),
    ("schema_info", pa.string()),
    ("source", pa.string()),
]
SCHEMA = pa.schema(COLUMNS)
# jsonb is exported as Postgres' canonical text so the parquet holds exactly what the DB holds
SELECT_COLUMNS = ", ".join(f"{name}::text" if name in ("tags_json", "raw") else name for name, _ in COLUMNS)
RAW_IDX = [name for name, _ in COLUMNS].index("raw")


def uid_fingerprint(uids) -> int:
    return sum(int(hashlib.md5(bytes(uid)).hexdigest()[:15], 16) for uid in uids)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=HETZNER_ENDPOINT,
        aws_access_key_id=os.getenv("HETZNER_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("HETZNER_SECRET_ACCESS_KEY"),
    )


def get_bucket() -> str:
    bucket = os.getenv("HETZNER_BUCKET_NAME")
    if not bucket:
        raise ValueError("Missing env var HETZNER_BUCKET_NAME")
    return bucket


def _connect(db_connector):
    conn = psycopg2.connect(db_connector.uri)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def get_db_day_stats(db_connector) -> dict:
    """{'YYYY-MM-DD': (rows, uid_fingerprint)} for offchain attestations before today (UTC)."""
    conn = _connect(db_connector)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT ("time" AT TIME ZONE 'UTC')::date AS day, count(*), sum({FINGERPRINT_SQL})
                FROM public.attestations
                WHERE is_offchain
                    AND "time" < date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                GROUP BY 1
            """)
            return {d.isoformat(): (int(n), int(fp)) for d, n, fp in cur.fetchall()}
    finally:
        conn.close()


def _manifest_key(day: str) -> str:
    return f"{PREFIX}/date={day}/_manifest.json"


def load_manifests(s3, bucket) -> dict:
    """{'YYYY-MM-DD': manifest} for every day that has a manifest in the bucket."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{PREFIX}/"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith("/_manifest.json")]

    def fetch(key):
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    with ThreadPoolExecutor(max_workers=16) as ex:
        manifests = list(ex.map(fetch, keys))
    return {m["day"]: m for m in manifests}


def is_day_complete(db_stats: tuple, manifest) -> bool:
    if not manifest:
        return False
    rows, fp = db_stats
    return manifest.get("db_rows") == rows and manifest.get("db_uid_fingerprint") == str(fp)


def find_incomplete_days(db_stats: dict, manifests: dict) -> list:
    return sorted(day for day, stats in db_stats.items() if not is_day_complete(stats, manifests.get(day)))


def _put_manifest(s3, bucket, manifest):
    manifest["rows"] = sum(p["rows"] for p in manifest["parts"])
    manifest["uid_fingerprint"] = str(sum(int(p["uid_fingerprint"]) for p in manifest["parts"]))
    s3.put_object(
        Bucket=bucket,
        Key=_manifest_key(manifest["day"]),
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )


def _backed_up_uids(s3, bucket, manifest) -> set:
    uids = set()
    with tempfile.TemporaryDirectory() as tmpdir:
        for part in manifest["parts"]:
            local = os.path.join(tmpdir, "part.parquet")
            s3.download_file(bucket, part["key"], local)
            uids.update(pq.read_table(local, columns=["uid"]).column("uid").to_pylist())
            os.remove(local)
    return uids


def _write_part(s3, bucket, manifest, rows: list, key: str):
    """Write rows to parquet, verify the file locally, upload, verify the upload, then record it in the manifest."""
    columns = list(zip(*rows))
    table = pa.table(
        {name: pa.array(columns[i], type=typ) for i, (name, typ) in enumerate(COLUMNS)},
        schema=SCHEMA,
    )
    expected_fp = uid_fingerprint(columns[0])

    with tempfile.TemporaryDirectory() as tmpdir:
        local = os.path.join(tmpdir, "part.parquet")
        pq.write_table(table, local, compression="zstd", compression_level=9)

        check = pq.read_table(local)
        if check.num_rows != len(rows):
            raise RuntimeError(f"{key}: wrote {len(rows)} rows, read back {check.num_rows}")
        if uid_fingerprint(check.column("uid").to_pylist()) != expected_fp:
            raise RuntimeError(f"{key}: uid fingerprint mismatch after read-back")
        if check.column("raw").null_count:
            raise RuntimeError(f"{key}: raw contains NULLs")

        size = os.path.getsize(local)
        with open(local, "rb") as f:
            sha256 = hashlib.file_digest(f, "sha256").hexdigest()

        upload_file_to_s3(bucket, key, local, empty_cf_cache=False, destination="hetzner")

    remote_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if remote_size != size:
        raise RuntimeError(f"{key}: uploaded size {remote_size} != local size {size}")

    manifest["parts"].append({
        "key": key,
        "rows": len(rows),
        "uid_fingerprint": str(expected_fp),
        "bytes": size,
        "sha256": sha256,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    # manifest is rewritten after every part, so an interrupted run keeps its progress
    _put_manifest(s3, bucket, manifest)


def backup_day(db_connector, s3, bucket, day: str, manifest, run_ts: str) -> dict:
    """
    Back up every offchain attestation of `day` that is not yet in the bucket.
    Returns a summary; 'missing_raw' > 0 means rows exist in the DB without raw and without a backup.
    """
    if manifest is None:
        manifest = {
            "table": "oli.public.attestations",
            "filter": "is_offchain",
            "partition": "UTC date of attestations.time",
            "day": day,
            "parts": [],
        }
    backed_up = _backed_up_uids(s3, bucket, manifest) if manifest["parts"] else set()

    start = datetime.combine(date.fromisoformat(day), time.min, tzinfo=timezone.utc)
    seen_rows, seen_fp, missing_raw, exported, nulled = 0, 0, 0, 0, 0
    buffer, backed_up_with_raw = [], []

    def flush():
        nonlocal buffer, exported, nulled
        if buffer:
            key = f"{PREFIX}/date={day}/part-{run_ts}-{len(manifest['parts']):04d}.parquet"
            _write_part(s3, bucket, manifest, buffer, key)
            exported += len(buffer)
            # part is verified locally and in the bucket, raw can go from Postgres
            nulled += _null_raw(db_connector, [r[0] for r in buffer])
            buffer = []

    conn = _connect(db_connector)
    try:
        # no ORDER BY: avoids a sort that would spill to disk on the DB host
        with conn.cursor(name=f"oli_raw_backup_{day}") as cur:
            cur.itersize = FETCH_ROWS
            cur.execute(f"""
                SELECT {SELECT_COLUMNS}
                FROM public.attestations
                WHERE is_offchain
                    AND "time" >= %(start)s AND "time" < %(end)s
            """, {"start": start, "end": start + timedelta(days=1)})

            for row in cur:
                row = tuple(bytes(v) if isinstance(v, memoryview) else v for v in row)
                uid = row[0]
                seen_rows += 1
                seen_fp += uid_fingerprint([uid])
                if uid in backed_up:
                    if row[RAW_IDX] is not None:
                        backed_up_with_raw.append(uid)
                    continue
                if row[RAW_IDX] is None:
                    missing_raw += 1
                    continue
                buffer.append(row)
                if len(buffer) >= PART_ROWS:
                    flush()
            flush()
    finally:
        conn.close()
    nulled += _null_raw(db_connector, backed_up_with_raw)

    if missing_raw == 0:
        # every DB uid of this day is now in the bucket
        manifest["db_rows"] = seen_rows
        manifest["db_uid_fingerprint"] = str(seen_fp)
        manifest["verified_at"] = datetime.now(timezone.utc).isoformat()
        _put_manifest(s3, bucket, manifest)

    return {"day": day, "db_rows": seen_rows, "exported": exported, "raw_nulled": nulled, "missing_raw": missing_raw}


def verify_parts_exist(s3, bucket, manifests: dict) -> list:
    """Returns the part keys whose object is missing or has a different size than recorded."""
    parts = [p for m in manifests.values() for p in m["parts"]]

    def check(part):
        try:
            return None if s3.head_object(Bucket=bucket, Key=part["key"])["ContentLength"] == part["bytes"] else part["key"]
        except ClientError:
            return part["key"]

    with ThreadPoolExecutor(max_workers=16) as ex:
        return [k for k in ex.map(check, parts) if k]


# --- dropping raw from Postgres once it is backed up ---

NULL_BATCH_ROWS = 50_000
MAX_SLOT_LAG_BYTES = 2 * 1024**3  # pause while a replication slot (web3 subscription) is further behind
SLOT_LAG_TIMEOUT_S = 3600


def _wait_for_replication_slots(cur):
    waited = 0
    while True:
        cur.execute("SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)), 0) FROM pg_replication_slots")
        lag = int(cur.fetchone()[0])
        if lag <= MAX_SLOT_LAG_BYTES:
            return
        if waited >= SLOT_LAG_TIMEOUT_S:
            raise RuntimeError(f"Replication slot lag still {lag / 1024**3:.1f} GB after {waited}s, stopping")
        print(f"Replication slot lag {lag / 1024**3:.1f} GB, waiting...")
        sleep(30)
        waited += 30


def _null_raw(db_connector, uids: list) -> int:
    """
    Set raw = NULL for these uids. Separate connection, so the
    day's streaming cursor stays untouched. last_updated_time is left as is on purpose: the
    attestation itself doesn't change, only where raw is stored.
    """
    nulled = 0
    conn = _connect(db_connector)
    try:
        with conn.cursor() as cur:
            for i in range(0, len(uids), NULL_BATCH_ROWS):
                _wait_for_replication_slots(cur)
                cur.execute("""
                    UPDATE public.attestations
                    SET raw = NULL
                    WHERE uid = ANY(%(uids)s)
                        AND is_offchain
                        AND raw IS NOT NULL
                """, {"uids": [psycopg2.Binary(u) for u in uids[i:i + NULL_BATCH_ROWS]]})
                nulled += cur.rowcount
                conn.commit()
    finally:
        conn.close()
    return nulled
