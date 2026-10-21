# Drop unused VERA tags from `vw_oli_label_pool_gold_v2`

Status: proposed, not started (analysis 2026-10-21)

## Problem

`refresh_materialized_views` in `oli_airtable` went from ~3 min (before 2026-10-07) to ~35 min per run, with spikes up to ~110 min.

Cause: the VERA backfill (attester `0xa725…0b56`, 2026-10-06 → 10-14) added ~29.6M attestations, mostly with 8–10 tags each. `attestations` grew to ~51M rows (43 GB), and `vw_oli_label_pool_gold_v2` (one row per tag) grew to ~235M rows (39 GB).

(The spikes were also caused by leaked `pd.read_sql(..., engine.connect())` sessions holding locks; fixed in `ff931ba9`.)

## Proposal

Filter the VERA tags that nothing reads out of `gold_v2`:

| tag_id | share of gold_v2 rows (pg_stats) |
|---|---|
| `source_code_verified` | 11.5% |
| `_source` | 11.4% |
| `code_language` | 9.4% |
| `code_compiler` | 9.3% |
| `deployment_block` | 9.0% |
| **total** | **~50%** |

Change in the `ranked_labels` CTE of `gold_v2`:

```sql
AND l.tag_id NOT IN ('source_code_verified', '_source', 'code_language', 'code_compiler', 'deployment_block')
```

The filter sits in the `WHERE` clause before the `row_number()` window, so both the ranking and the write roughly halve.

## Why it is safe

- **Ranking stays the same for the other tags.** `gold_v2` ranks per `(address, chain_id, tag_id)` (`PARTITION BY l.address, l.chain_id, l.tag_id ORDER BY t.confidence DESC, l."time" DESC`), so removing some tags doesn't change the winning value of any other tag.
- **`pivoted_v2` keeps its shape.** It pivots all five tags into columns (`source_code_verified`, `_source`, `code_language`, `code_compiler`, `deployment_block`); these would simply become NULL.
- **No reader in the repo uses those columns.** Every query on `pivoted_v2` lists its columns explicitly:
  - `db_connector.get_oli_labels`, `get_labels_lite_db`, `get_labels_export_df` (labels / top-contracts JSONs)
  - `json_creation.create_export_project_labels_parquet`, `builder_api/api.py`
  - `adapter_stables_balances._discover_oli`, `other_qb_argot_tvs.py` (qb_argot reads `code_compiler`/`code_language` from `attestations.tags_json` directly, not from the views)
  - DB views `vw_oli_label_pool_gold_pivoted_cross_chain_v1`, `vw_apps_contract_level_materialized`, `vw_fact_stables_balances_v1`
- **The direct readers of `gold_v2` (`db_connector.py`) only use tags that stay:** `get_tags_inactive_projects`, `get_oli_labels_gold_by_owner_project`, `get_unlabelled_contracts(_v2)` and `get_labels_export_gold_table` (has no callers; it selects `source_code_verified`, which would become NULL).
- **The owner-project remap** (`airtable_read_depreciated_owner_project`) re-attests all gold tags for an address. With the filter it stops re-attesting these VERA tags under the GTP key, which is preferable; the VERA attestations themselves stay in `attestations`.

**Not checked:** consumers outside this repo (BI tools, ad-hoc queries, external DB users) that select these tags or columns directly.

**Tags that must stay** (VERA provides them, but they're used): `contract_name`, `deployment_tx`, `deployer_address`, `is_contract`, `_comment` (read by `get_unlabelled_contracts`).

## Implementation

A materialized view's definition can't be altered in place, so `gold_v2` has to be dropped and recreated together with everything that depends on it:

```
vw_oli_label_pool_gold_v2 (m)
└── vw_oli_label_pool_gold_pivoted_v2 (m)
    ├── vw_fact_stables_balances_v1 (v)
    └── vw_oli_label_pool_gold_pivoted_cross_chain_v1 (m)
        └── vw_apps_contract_level_materialized (m)
```

Steps:
1. Dump the current definitions (`pg_get_viewdef`) plus indexes and grants of all five objects. `gold_v2` and `pivoted_v2` aren't in the repo yet; add them under `backend/src/queries/postgres/oli/`. `pg_get_viewdef` needs a lock on `gold_v2`, so don't run it during a refresh.
2. In one transaction: `DROP MATERIALIZED VIEW vw_oli_label_pool_gold_v2 CASCADE;`, then recreate `gold_v2` (with the filter), `pivoted_v2`, `pivoted_cross_chain_v1`, `vw_apps_contract_level_materialized`, `vw_fact_stables_balances_v1`, their indexes and grants.
3. Run it outside the `oli_airtable` / apps JSON schedule; readers lose the views until the rebuild finishes.
4. Verify: row count of `gold_v2` roughly halves; `pivoted_v2` row count unchanged; the task duration of `refresh_materialized_views` drops.

## Optional follow-ups

- `REFRESH MATERIALIZED VIEW CONCURRENTLY` (needs a unique index) so readers don't block the refresh and vice versa. It isn't faster.
- Delete the unused `get_labels_export_gold_table`.

## Reference: current `gold_v2` definition (2026-10-21)

```sql
WITH ranked_labels AS (
    SELECT decode(SUBSTRING(lower(l.address) FROM 3), 'hex') AS address,
           l.chain_id AS caip2, l.tag_id, l.tag_value, l.attester,
           EXTRACT(epoch FROM l."time")::integer AS time_created,
           t.confidence,
           row_number() OVER (PARTITION BY l.address, l.chain_id, l.tag_id ORDER BY t.confidence DESC, l."time" DESC) AS rn
    FROM labels l
    JOIN oli_trust_table t
      ON concat('0x', encode(l.attester, 'hex')) = lower(t.address::text)
     AND (t.tag_id = '*' OR t.tag_id = l.tag_id)
     AND (t.chain_id = '*' OR t.chain_id = l.chain_id)
    WHERE lower(l.address) ~ '^0x[0-9a-f]+$'
      AND t.confidence >= 0.1
      AND l.tag_id IN (SELECT tag_id FROM oli_tags)
      AND (l.tag_id <> 'usage_category' OR l.tag_value IN (SELECT category_id FROM oli_categories))
      AND (l.tag_id <> 'owner_project' OR l.tag_value IN (SELECT name FROM oli_oss_directory))
      -- proposed:
      -- AND l.tag_id NOT IN ('source_code_verified', '_source', 'code_language', 'code_compiler', 'deployment_block')
)
SELECT rl.address, smc.origin_key, rl.caip2, rl.tag_id, rl.tag_value, rl.attester, rl.time_created, rl.confidence
FROM ranked_labels rl
LEFT JOIN sys_main_conf smc ON rl.caip2 = smc.caip2
WHERE rl.rn = 1;
```
