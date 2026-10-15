# ETH Asset Page — Data Methodology

Data behind the "ETH the asset" page. Four JSONs, all regenerated hourly by the Airflow DAG `api_eth_asset` (runs at `:35`, after the hourly CoinGecko price load):

| JSON | Content |
|---|---|
| `v1/eth_asset/kpis.json` | 5 KPI charts (timeseries per timeframe, current value, % change) |
| `v1/eth_asset/stats.json` | Single values the frontend extrapolates per second (`total_wallets`, `eth_per_person`) |
| `v1/eth_asset/properties.json` | "ETH is 6 things at once" section: yield breakdown, scarcity, collateral, ETH paid as gas |
| `v1/eth_asset/market_caps.json` | "How big is it really" section: current market cap of ETH vs. top cryptos, commodities and companies |

Code: `backend/airflow/dags/api/api_eth_asset.py`, `backend/src/api/eth_asset_json.py`, `backend/eim/adapters/adapter_eth_supply.py`, `backend/eim/adapters/adapter_eth_collateral.py`, `backend/eim/adapters/adapter_reference_data.py`, `backend/src/db_connector.py` (`get_eth_burned`, `get_eth_staking_cl_daily`).

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
| Yield breakdown | ethsupply.fyi, growthepie |
| Scarcity | ultrasound.money (ETH), World Gold Council (gold), FRED (US dollar) |
| Collateral | DefiLlama, growthepie |
| ETH paid as gas | growthepie |
| Market caps | CoinGecko (crypto), CompaniesMarketCap (companies, gold, silver, platinum), EIA via Our World in Data + Yahoo Finance (oil) |

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

## 3. Properties (`properties.json`)

**Yield breakdown**: the current staking yield split into issuance, tips and MEV. `total` is identical to `staking_yield.current` in `kpis.json`.
- `issuance`: CL yield, trailing 24h average of hourly ethsupply.fyi data (same as the hourly KPI).
- `tips` and `mev`: 7-day average of daily EL rewards ÷ staked ETH, split per block (Dune query 8951204, daily since the Merge):
  - Locally built block: the proposer receives the priority fees. All of it counts as `tips`.
  - MEV-boost block: the proposer receives the builder's payment. The payment first covers the block's priority fees (`tips`, capped at the payment); anything above that is `mev`.
  - Both parts are never negative and add up to the EL reward. Builders often pay less than the block's priority fees (they keep a margin), so `mev` is the smaller part. As of Oct 2026: tips 0.07%, MEV 0.01%.

**Scarcity**: annual change in the total stock of each asset, as a fraction.
- ETH: supply change over the last 30 days (latest hourly supply vs. the start-of-day supply 30 days ago), annualized. Source: ultrasound.money.
- Gold: World Gold Council above-ground stock. The figure ("Total above-ground stock (end-Q2 2026): 222,600 tonnes") is scraped daily at 06:00 UTC from gold.org by `AdapterReferenceData`. It's stored as `eim_fact.gold_stock_tonnes` (origin_key `gold`) at the end date of its period; WGC updates it quarterly. The annual change compares the latest stock with the latest stored value that is at least a year older (else the oldest), annualized. The year-end values for 2024 (216,265 t) and 2025 (219,891 t) were seeded once. Until mid-2027, the comparison is therefore 2026-06-30 vs. 2024-12-31 (+1.96%/yr); after that it is a clean year-over-year change. If the page layout changes, the scrape fails and the last stored value is used.
- US dollar: M2 money supply (FRED `M2SL`, monthly, seasonally adjusted), latest month vs. the same month a year earlier.

**Collateral**: ETH-equivalent tokens deposited in DeFi on Ethereum + L2s with ETH as gas token (chains via `aliases_defillama` in `main_config`). Daily snapshot from DefiLlama, converted to ETH with the latest hourly ETH price. Categories are partitioned so nothing is counted twice:
- `lending`: all ETH-equivalent tokens (ETH, WETH, LSTs, LRTs) in protocols of the DefiLlama category "Lending". This is DefiLlama TVL, i.e. supplied minus borrowed, which avoids inflating the number through looping.
- `dex`: all ETH-equivalent tokens in protocols of the category "Dexs".
- `liquid_staking`: ETH staked via protocols of the category "Liquid Staking" (Ethereum mainnet) **minus** the LST tokens already counted in lending and DEX. This is the liquid-staked ETH that is not deployed elsewhere.
- Only protocols with ≥ $5M TVL on the selected chains are fetched (35 lending, 25 DEX as of Oct 2026).
- Token classification: ETH/WETH = native. LSTs = the EIM `eth_derivatives` list plus wBETH, osETH, lsETH, ETHx and swETH. LRTs (ezETH, rsETH, rswETH, pufETH) count in lending/DEX but aren't subtracted from liquid staking.
- Known small overlap: LRTs hold LSTs underneath (e.g. rsETH backed by stETH), so about $0.9B of LRT collateral in lending is also partly inside the liquid staking total.
- Values as of 2026-10-15: 18.4M ETH ($43.6B) in total, 64% liquid staking, 34% lending, 1% DEX.
- Stored in `eim_fact`: `collateral_lending_eth`, `collateral_liquid_staking_eth`, `collateral_dex_eth`.

**ETH paid as gas**: daily sum of `fees_paid_eth` (and `fees_paid_usd`) from `fact_kpis` over all chains in `main_config` with `metadata_gas_token = 'ETH'`, `api_in_main` and deployment flag `PROD` (Ethereum + 17 L2s). `fees_paid_eth` exists for every chain, but for chains with another gas token it is only a currency conversion. Those chains are therefore excluded: Polygon PoS (POL), Mantle (MNT), Metis, Ronin, Gravity, Plume, Fraxtal, Celo (multiple gas tokens), and Starknet (ETH or STRK, which our data can't separate). The last 365 complete days are included. `chains` lists the chains with fees in the last 7 days.

---

## 4. Market caps (`market_caps.json`)

Current market cap in USD, refetched on every hourly run. Nothing is hardcoded, and nothing needs manual updates.

- **Crypto**: ETH, BTC, USDT, USDC from CoinGecko `/coins/markets`.
- **Companies**: the global top 10 by market cap from the CompaniesMarketCap CSV (`companiesmarketcap.com/?download=csv`, already in USD). New entrants appear automatically.
- **Commodities**:
  - Gold, silver, platinum: every commodity on the CompaniesMarketCap assets ranking (rows whose logo id ends in `.XM`). Their method is estimated above-ground stock × spot price.
  - Oil, which CompaniesMarketCap doesn't list: world proved reserves (EIA via Our World in Data, latest year read at runtime, currently 2021: ~1,659B barrels) × Brent price (Yahoo Finance `BZ=F`). Most of this oil is still underground, which is why it dwarfs everything else (~$170T).
- **ETH/BTC market cap ratio** (`eth_btc_ratio`): daily timeseries for the last 2,000 complete days (~5.5 years), values as of 00:00 UTC. `current` is the live ratio from CoinGecko. Our CoinGecko plan only serves 2 years of history, so the series is built from:
  - ETH: `fact_kpis.market_cap_usd` (origin_key `ethereum`, collected daily from CoinGecko since 2015).
  - BTC: Yahoo Finance `BTC-USD` daily close of the previous day × circulating supply from blockchain.com (`total-bitcoins` chart, interpolated to daily). Checked against CoinGecko over 2 years: identical on average, 0.16% standard deviation per day.
- **Logos**: downloaded the first time an item appears and served from `api.growthepie.com/v1/eth_asset/logos/<key>.png`. Sources: CoinGecko for coins, CompaniesMarketCap (128px) for companies and metals, financialmodelingprep.com for oil. `key` is derived from the ticker (e.g. `nvda`, `2222_sr`), so a new company in the top 10 gets its logo on its first run.
- If parsing the CompaniesMarketCap assets page breaks (page layout change), the task fails instead of publishing wrong numbers, and the previous JSON stays live.

---

## 5. Source reliability and fallbacks

| Source | Used for | If unavailable |
|---|---|---|
| ethsupply.fyi | Hourly staked ETH, CL rewards | Staked ETH falls back to Dune (lagging, effective balance); CL yield falls back to 7-day average of daily values; Discord alert |
| ultrasound.money | Supply (hourly + daily) | Task fails/retries; JSON tasks don't run, so the previous JSONs stay live |
| Dune | Daily staked ETH, EL rewards, hourly staked fallback | Daily EL data retried on the next hourly run until yesterday is loaded |
| World Bank | Population (stats) | Stats task fails; `kpis.json` is unaffected |
| DefiLlama | Collateral | Collateral task fails/retries; previous `properties.json` stays live |
| FRED | US dollar M2 | Properties task fails/retries; previous `properties.json` stays live |
| CoinGecko / CompaniesMarketCap / OWID / Yahoo Finance | Market caps | Market caps task fails/retries; previous `market_caps.json` stays live |
| gold.org (scrape) | Gold stock for scarcity | Reference data task fails; scarcity keeps using the last stored gold stock |
| Own `ethereum_blocks` | Burn | — |
