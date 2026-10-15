# ETH Asset Page — Data Methodology

Data behind the "ETH the asset" page. Two JSONs, both regenerated hourly by the Airflow DAG `api_eth_asset` (runs at `:35`, after the hourly CoinGecko price load):

| JSON | Content |
|---|---|
| `v1/eth_asset/kpis.json` | 5 KPI charts (timeseries per timeframe, current value, % change) |
| `v1/eth_asset/stats.json` | Single values the frontend extrapolates per second (`total_wallets`, `eth_per_person`) |

Code: `backend/airflow/dags/api/api_eth_asset.py`, `backend/src/api/eth_asset_json.py`, `backend/eim/adapters/adapter_eth_supply.py`, `backend/src/db_connector.py` (`get_eth_burned`, `get_eth_staking_cl_daily`).

---

## 1. KPI charts (`kpis.json`)

### Timeframes

| Toggle | Bucket | Points | Built from |
|---|---|---|---|
| 24h | 1 hour | 24 | hourly data |
| 7d | 3 hours | 56 | hourly data |
| 60d | 1 day | 60 | daily data |
| 1y | 7 days | 52 | daily data |
| 5y | calendar month | 60 | daily data |

- Only complete hours / days are used. Hourly windows end at the last complete hour, daily windows at yesterday.
- 3h and 7d buckets are counted backwards from the window end, so every bucket is complete. Timestamps are bucket starts (unix ms, UTC).
- 5y uses calendar months; the current month is a partial bucket.

### Aggregation, current value and % change

| Type | KPIs | Bucket value | `current` | `change_pct` |
|---|---|---|---|---|
| Stock | price, supply, % staked | last value in bucket | latest hourly value (same for all toggles) | vs. value one window ago |
| Rate | staking yield | average over bucket | latest hourly value (same for all toggles) | vs. value one window ago |
| Flow | ETH burned | sum over bucket | total within the selected window | vs. total of the previous window of equal length |

- `change_pct` is a fraction (0.05 = +5%) and is `null` for 5y, or when data doesn't cover the comparison window.
- Yields and % staked are fractions as well (0.0256 = 2.56%).

### KPI definitions and sources

*Public attribution* (the `sources` field in the JSON): Dune is only our query infrastructure. Those queries and the logic are ours, so they are attributed to growthepie.

| KPI | Sources in JSON |
|---|---|
| ETH price | CoinGecko |
| ETH supply | ultrasound.money |
| Staking yield | ethsupply.fyi, growthepie |
| % of supply staked | ethsupply.fyi, growthepie, ultrasound.money |
| ETH burned | growthepie |
| Total wallets | growthepie |
| ETH per person | ultrasound.money, World Bank |

**ETH price (USD)** — CoinGecko. Hourly in `fact_kpis_granular` (`price_usd`), daily in `fact_kpis`.

**ETH supply (ETH)** — ultrasound.money `supply-over-time` API. Daily from the `since_burn` series (`eim_fact.eth_supply_eth`, start-of-day snapshot), hourly from the `d30` series (last datapoint per hour).

**ETH burned (ETH)** — EIP-1559 base fee burn + EIP-4844 blob fee burn:
`Σ (base_fee_per_gas × gas_used + blob_base_fee × blob_gas_used) / 1e18`
- From Dencun (2024-03-14) onwards: growthepie's own Ethereum block data (`ethereum_blocks`), hourly and daily.
- 2021-08-05 (London) – 2024-03-13: one-off backfill from Dune `ethereum.blocks` (base fee burn only; no blobs before Dencun). Dune query 8951324.
- Cross-checked against ethsupply.fyi: 24h burn within 0.04%.

**% of supply staked** — staked ETH ÷ ETH supply.
Staked ETH = **actual balance** (not effective balance) of active validators. Pending deposits and validators in the activation queue are excluded.
- Hourly: ethsupply.fyi `/api/history?range=24h` (`activeBalanceGwei`, last datapoint per hour). Fallback if ethsupply.fyi is unavailable: Dune `beacon.epoch_summaries.active_balance` (query 8951203). This lags ~9h and is effective balance (~0.15% lower).
- Daily: Dune `beacon.validator_day_summaries`, sum of `start_balance` of active validators (start of day). Matches ethsupply.fyi active balance within 0.2% on every day of 2026.

**Staking yield** — annualized consensus-layer (CL) yield + execution-layer (EL) yield, shown as one aggregated value.

- *CL yield* = net CL rewards (issuance − consensus penalties) × 365 ÷ staked ETH. Sources by period:

  | Period | Source |
  |---|---|
  | 2020-12-01 – 2025-05-06 | Dune `beacon.validator_day_summaries.reward_change` (query 8951204) |
  | 2025-05-07 – 2026-01-01 | Theoretical issuance (see below) |
  | 2026-01-02 – now | ethsupply.fyi (`issuanceWei − consensusPenaltiesWei` per epoch), hourly, rolled up to complete days |

  Dune's `reward_change` is unreliable after Pectra (May 2025): ~70 days with spikes up to 68% APR or drops to ~0.7%, likely consolidations/deposits counted as rewards and incomplete days. It's therefore not used after 2025-05-06.

  *Theoretical fill:* the protocol issues `64 × √(total active balance in gwei)` gwei per epoch at full participation. This is multiplied by a participation factor and 225 epochs/day. The factor is linearly interpolated from 0.9754 (median Jan–May 2025, Dune) to 0.9846 (Jan 2026, ethsupply.fyi). The transitions are smooth: 2.763% → 2.770% on 2025-05-06/07, and 2.739% → 2.744% at the turn of the year. 2026-01-01 also uses the theoretical value because ethsupply.fyi's first interval only covers ~8h.

  Manual fix: 2023-04-15/16 (Shapella week). Dune split rewards unevenly across the day boundary; the two-day total is spread evenly over both days.

- *EL yield* = rewards received by block proposers × 365 ÷ staked ETH. Per block: the MEV-boost payment (last transaction, sent by the block's fee recipient/builder) if present, otherwise the priority fees. Source: Dune `ethereum.blocks` + `ethereum.transactions` (query 8951204), daily from the Merge (2022-09-15); 0 before. Large real spikes on MEV-heavy days (e.g. USDC depeg 2023-03-11, Oct 10 2025 liquidations).

- *Smoothing:*
  - Daily charts (60d, 1y, 5y): 7-day rolling average of daily CL + EL yield.
  - Hourly charts (24h, 7d): hourly CL yield (trailing 24h average) + 7-day average EL yield (EL is only available daily). If hourly CL data is missing, the 7-day average daily CL yield is used.

### Storage

| Data | Table | metric_key |
|---|---|---|
| Hourly | `fact_kpis_granular` (granularity `hourly`, origin_key `ethereum`) | `price_usd`, `eth_supply_eth`, `eth_burned_eth`, `eth_staked_eth`, `staking_rewards_cl_eth` |
| Daily | `eim_fact` (origin_key `ethereum`) | `eth_supply_eth`, `eth_burned_eth`, `eth_staked_eth`, `staking_rewards_cl_eth`, `staking_rewards_el_eth`, `staking_yield_cl`, `staking_yield_el` |
| Daily | `fact_kpis` | `price_usd` |

The daily Dune staking query only writes `eth_staked_eth`, `staking_rewards_el_eth` and `staking_yield_el`. CL values come from ethsupply.fyi.

---

## 2. Stats (`stats.json`)

Each value comes with a `timestamp` (unix ms) and a `per_second_increase`. The frontend shows `value + (now − timestamp) × per_second_increase`.

**Total wallets** — the number of unique addresses that ever initiated a transaction on Ethereum mainnet. This is the same number as the active addresses lifetime achievement on the Ethereum chain page (`select_total_aa.sql.j2`, HyperLogLog union over `fact_active_addresses_hll`, estimation error ~0.3%).
- `value`: all complete days, so `timestamp` = today 00:00 UTC.
- `per_second_increase`: average growth over the last 30 days. A 7-day window is noticeably noisier because of HLL estimation error.
- Source: growthepie.

**ETH per person** — ETH supply ÷ world population.
- Supply: latest hourly supply (ultrasound.money); `timestamp` = that hour.
- World population: World Bank Population Estimates and Projections (`SP.POP.TOTL`, source 40, mid-year values incl. projections), linearly interpolated between July 1 values.
- `per_second_increase` = S′/P − S·P′/P², where S′ is the average supply change per second over the last 7 days (hourly supply) and P′ is the population growth per second of the current interpolation segment.
- Values as of Oct 2026: ~0.0147 ETH/person, increase ~+2.7e-13 ETH/s. Supply grows ~2.9k ETH/day and population ~186k/day, so the per-second change only shows around the 13th decimal.
- `details` includes the underlying supply, population and their per-second rates.

---

## 3. Source reliability and fallbacks

| Source | Used for | If unavailable |
|---|---|---|
| ethsupply.fyi | Hourly staked ETH, CL rewards | Staked ETH falls back to Dune (lagging, effective balance); CL yield falls back to 7-day average of daily values; Discord alert |
| ultrasound.money | Supply (hourly + daily) | Task fails/retries; JSON tasks don't run, so the previous JSONs stay live |
| Dune | Daily staked ETH, EL rewards, hourly staked fallback | Daily EL data retried on the next hourly run until yesterday is loaded |
| World Bank | Population (stats) | Stats task fails; `kpis.json` is unaffected |
| Own `ethereum_blocks` | Burn | — |
