# Stablecoin balances adapter: status

As of 2026-10-15. Data in the DB covers 2026-10-01 to 2026-10-07 (manual trial runs); nothing has run since.

**Goal:** daily stablecoin balances, backfilled at least 3 months, for every address with an OLI `owner_project` plus the larger holders found by discovery, so balances can be attributed to applications and categories.

**Where it stands:** the pipeline works end to end on Ethereum, Polygon PoS and Celo, but the DAG is not deployed, history is only 7 days, and the OLI source is capped at 2,000 addresses per chain. Polygon coverage is the weak spot.

Architecture write-up with diagrams and trial results: https://claude.ai/code/artifact/ff7e1053-75ce-422b-bbe4-0682213a8eb6 (Claude Docs; Discovery and Measurement sections predate the 7-day/weekly cadence change below).

---

## Coverage on 2026-10-07

| Chain | Candidates | Stablecoin supply | Held by tracked addresses | Held by OLI-labeled owners |
| --- | --- | --- | --- | --- |
| Ethereum | 9,094 | $163.6B | 80.1% | 41.0% |
| Celo | 2,522 | $0.144B | 88.7% | 7.0% |
| Polygon PoS | 7,363 | $3.0B | 53.4% | 15.7% |

- Labeled share barely moved while tracked share grew: about $64B on Ethereum sits at tracked addresses with no OLI owner. The labeling list is in `backend/output/stables_unlabeled_addresses_2026-10-07.csv` (9,302 addresses, gitignored); 735 addresses cover 80% of the unlabeled value per chain.
- Most labeled value is exchanges, issuers' own contracts and bridges (Ethereum: binance $38.4B, makerdao $10.5B, okx $4.1B). DeFi apps are a small share.

---

## What exists

### Code (staged on `main`, not committed)

| File | Role |
| --- | --- |
| `backend/src/adapters/adapter_stables_balances.py` | `AdapterStablecoinBalances`: discovery + measurement |
| `backend/airflow/dags/metrics/metrics_stables_balances.py` | DAG `metrics_stables_balances`, daily 01:45 UTC, 3 chains, `days=30` |
| `backend/src/queries/postgres/stables/create_stables_balances.sql` | DDL: 2 tables + view (already applied, incl. `last_full_scan` column) |
| `backend/src/queries/postgres/stables/stables_balances_chain_summary.sql.j2` | Daily totals per chain vs supply |
| `backend/src/queries/postgres/stables/stables_balances_unmapped_top.sql.j2` | Unlabeled holders ranked by balance |
| `backend/src/queries/dune/stables_balances_holder_discovery.sql` | Saved Dune query 8915328 (top receivers per token in a window) |
| `backend/src/queries/dune/stables_balances_top_holders.sql` | Saved Dune query 8915430 (all-time net transfers; fallback only) |
| `backend/src/currency_config.py` | Added GHS, BOB, PYG, UYU, VES (separate fix, see below) |

`.gitignore` also shows as modified; that change predates this project.

### Tables

| Object | Holds |
| --- | --- |
| `sys_stables_balances_candidates` | One row per (origin_key, address): discovery `sources`, discovery dates, `measured_from` / `measured_to`, `last_full_scan` |
| `fact_stables_balances` | End-of-day balance per (date, origin_key, token_id, address); non-zero rows only |
| `vw_fact_stables_balances_v1` | Balances + USD (price from `vw_fact_stables_mcap_v2`) + issuer + OLI owner_project / usage_category / main category |

Our DB user can't `ALTER` these tables (not owner); schema changes must be run by you.

### How it works

**Discovery** (which addresses to measure; every address found stays a candidate):

| Source | What | Cadence | Cost |
| --- | --- | --- | --- |
| `activity` | Top 1,000 per ranking by gas / tx count / active addresses, last 30 days | Daily | Postgres |
| `oli` | OLI addresses with `owner_project`, most active first, **capped at `oli_limit` = 2,000** | Daily | Postgres |
| `dune` | Top 1,000 receivers per token over the **last 7 days**, at least $100k received | Daily | ~1 Dune credit per chain |
| `blockscout` | Current top 1,000 holders per token with at least 1% of chain supply, at least $10k | First run per chain, then Mondays | Free (PRO key) |
| `dune_history` | Same as `dune` over yearly windows back 6 years (`offset_days`) | Manual one-time seed | ~2 min medium engine per window |
| `dune_holders` | All-time net transfers per token | Manual fallback only | ~60 credits on Ethereum; times out on Polygon and Celo |

**Measurement:** `balanceOf()` via Multicall3 at the last block of each day (`first_block_of_day(D+1) - 1`). Up to 1,000 balances per call, 6 calls in parallel, halving automatically when RPCs reject a batch (Polygon settles at 500). Each address gets a full scan of all mapped tokens every 7 days and only its held pairs in between; newly funded pairs found by a full scan are backfilled since the previous full scan. Addresses in `exclude_balances` of the stables config are never measured.

**RPCs:** non-special RPCs from `sys_rpc_config` only (active first, then inactive as fallback); paid `special_use` RPCs are never called. Failing RPCs move to the back of the list.

---

## Open issues

1. **Polygon PoS at 53%.** Blockscout treats Polygon as a "featured chain" that needs the Builder, Business or Pro plan (current key gets HTTP 402). The `dune_history` seed was built for this but has not run: the DB connection dropped on 2026-10-08 during its first window. In a test, the most recent year alone lifted Polygon to ~58%.
2. **OLI coverage is capped.** `oli_limit` = 2,000 per chain, but Ethereum has 18,352 OLI addresses with an `owner_project` (Polygon 4,598, Celo 903). The goal needs all of them.
3. **Backfill is expensive in its current form.** Dates before an address's `measured_from` are always full scans. Extending ~9k Ethereum addresses back 90 days would be ~1,100 calls per day of history, ~90k calls in total.
4. **Archive depth of free RPCs is unverified beyond 7 days.** On Ethereum, drpc, tenderly, sentio and mevblocker served 7-day-old state; pocket, publicnode, blockpi and several others are pruned. Polygon: drpc and Tenderly's gateway work. 90 days needs a check per chain.
5. **Blockscout key exposure.** The key appeared in a log once (error URLs carried it as a param; errors are redacted now). Rotate it. The adapter reads `BLOCKSCOUT_API_KEY_PRO` (Airflow) or `BLOCKSCOUT_PRO` (local `.env`).
6. **Keyed endpoints in `sys_rpc_config`.** Inactive, non-special rows include Alchemy, Tatum, QuickNode and RPCfast URLs with keys. The adapter may call them as a fallback. If any are paid, flag them `special_use = true`.
7. **Undecided: issuer-owned contracts and CEX wallets.** Whether they count as "held by apps". On Ethereum, issuer-owned contracts hold ~$8.6B and binance alone $38.4B. The view's issuer flag is per owner, not per token (binance shows as an issuer because of BUSD), so it needs a per-token comparison before use.
8. **FX backfill.** The 5 added currencies only get rates from the next `metrics_currency_conversion` run; past dates need a manual backfill of that DAG, then their stablecoins get USD values in both `vw_fact_stables_mcap_v2` and this view.

---

## Next steps toward the goal

1. **Commit on a branch.** Everything is staged on `main`; branch first, then commit (`feat: stablecoin balances adapter`).
2. **Make OLI coverage complete.** Drop the cap for the goal (`oli_limit` very high or remove the `LIMIT`); weekly full scans then cost ~18k × 124 / 1,000 ≈ 2.3k calls on Ethereum, sparse days stay cheap.
3. **Make backward backfill sparse.** Reuse the forward logic in the backward pass: full scan on the first (newest) backfill day, then only held pairs, with a full scan every 7 days going back. That brings a 90-day Ethereum backfill from ~90k calls down to roughly 15k.
4. **Verify 90-day archive access** on each chain's free RPCs before running the backfill.
5. **Fix Polygon.** Either run the `dune_history` seed once the DB is reachable (command below), or upgrade the Blockscout plan.
6. **Prune small addresses.** "Bigger addresses" needs a rule, e.g. drop non-OLI candidates whose balances stayed below $10k across N full scans. Not built yet.
7. **Backfill 3 months, then deploy the DAG** with `days` kept at 30 (it only measures missing days per address, so a larger window costs nothing once filled).
8. **Labeling.** Hand the CSV to the labeling team; consider storing Blockscout's name tag and `is_contract` on candidates (two new columns) as a head start.

---

## Runbook

```python
# from backend/, with the DB tunnel open
from dotenv import load_dotenv; load_dotenv('.env')
from src.db_connector import DbConnector
from src.adapters.adapter_stables_balances import AdapterStablecoinBalances
ad = AdapterStablecoinBalances({}, DbConnector())

# daily run as the DAG does it (one chain)
ad.extract({'origin_keys': ['celo'], 'days': 7})

# Polygon one-time seed (6 yearly Dune windows), then measure
ad.extract({'origin_keys': ['polygon_pos'], 'days': 7, 'sources': ['dune_history'], 'history_years': 6})

# measure only, no discovery
ad.extract({'origin_keys': ['ethereum'], 'days': 90, 'discover': False})
print(ad.rpc_calls)
```

Coverage check: render `stables_balances_chain_summary.sql.j2` with `origin_keys` and `days`, or query `vw_fact_stables_balances_v1` directly.

### Things that bit during the trial

- The DB tunnel on `localhost:5432` dropped several times; runs fail with `SSL SYSCALL error: EOF detected`. Progress is saved per day, so rerunning resumes.
- Stopping a local run does not stop a Dune execution already submitted. Cancel it via `POST https://api.dune.com/api/v1/execution/<id>/cancel`.
- Dune executions stop at 30 minutes; all-time scans of USDC/USDT on Polygon and Celo hit that limit.
- Public RPCs: Polygon caps eth_call responses at 100 KB (~500 balances); many Ethereum/Polygon public nodes are pruned and fail on historical blocks.
- `exclude_balances` addresses (e.g. Tether treasury, Polygon USDC.e's $501M) must not be measured, or coverage exceeds 100% for that token.
