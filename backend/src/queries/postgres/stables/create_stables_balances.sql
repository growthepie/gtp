-- Stablecoin balances of tracked addresses (contracts and EOAs): discovery metadata, daily balances, and labeled USD view.
-- Written by src/adapters/adapter_stables_balances.py (DAG: metrics_stables_balances).
-- upsert_table() runs with create_table=False, so run this once manually before the first DAG run.

-- Discovery + measurement progress per (origin_key, address).
-- measured_from / measured_to is a contiguous range of dates for which balances of ALL mapped
-- stablecoins were read via balanceOf. A balance row missing inside this range means 0.
CREATE TABLE IF NOT EXISTS public.sys_stables_balances_candidates (
    origin_key          varchar     NOT NULL,
    address             bytea       NOT NULL,
    sources             varchar,                -- comma separated, e.g. 'activity,oli'
    first_discovered    date,
    last_discovered     date,
    measured_from       date,
    measured_to         date,
    last_full_scan      date,                   -- last date ALL mapped tokens were read; in between only held pairs are read
    pruned_on           date,                   -- set when a small, unlabeled candidate stops being measured (see prune())
    PRIMARY KEY (origin_key, address)
);
-- added after the first deploy:
ALTER TABLE public.sys_stables_balances_candidates ADD COLUMN IF NOT EXISTS last_full_scan date;
ALTER TABLE public.sys_stables_balances_candidates ADD COLUMN IF NOT EXISTS pruned_on date;

-- End-of-day stablecoin balance per address. Sparse: only non-zero balances are stored.
CREATE TABLE IF NOT EXISTS public.fact_stables_balances (
    "date"      date        NOT NULL,
    origin_key  varchar     NOT NULL,
    token_id    varchar     NOT NULL,
    address     bytea       NOT NULL,
    balance     numeric     NOT NULL,           -- decimals-adjusted token units
    PRIMARY KEY ("date", origin_key, token_id, address)
);

CREATE INDEX IF NOT EXISTS idx_fsb_origin_key_date ON public.fact_stables_balances (origin_key, "date");
CREATE INDEX IF NOT EXISTS idx_fsb_address ON public.fact_stables_balances (address, origin_key);

-- Balances with USD value and OLI attribution.
-- price_usd is derived from vw_fact_stables_mcap_v2 (value_usd / value) so FX handling stays in one place.
-- issuer_project = stablecoin issuer; owner_project = application holding the balance.
CREATE OR REPLACE VIEW public.vw_fact_stables_balances_v1 AS
WITH prices AS (
    SELECT DISTINCT ON ("date", origin_key, token_id)
        "date",
        origin_key,
        token_id,
        value_usd / NULLIF(value, 0) AS price_usd
    FROM public.vw_fact_stables_mcap_v2
    WHERE metric_key IN ('total_supply', 'track_on_l1')
      AND value > 0
    ORDER BY "date", origin_key, token_id, metric_key
)
SELECT
    b."date",
    b.origin_key,
    b.token_id,
    s.symbol,
    s.fiat,
    s.owner_project AS issuer_project,
    b.address,
    b.balance,
    b.balance * COALESCE(p.price_usd, CASE WHEN s.fiat = 'usd' THEN 1 END) AS balance_usd,
    l.contract_name,
    l.owner_project,
    l.usage_category,
    cat.main_category_id AS main_category,
    l.is_contract,
    l.is_eoa
FROM public.fact_stables_balances b
LEFT JOIN public.sys_stables_v2 s ON s.token_id = b.token_id
LEFT JOIN prices p ON p."date" = b."date" AND p.origin_key = b.origin_key AND p.token_id = b.token_id
LEFT JOIN public.vw_oli_label_pool_gold_pivoted_v2 l ON l.address = b.address AND l.origin_key = b.origin_key
LEFT JOIN public.oli_categories cat ON cat.category_id = l.usage_category;
