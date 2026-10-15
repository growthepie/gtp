-- Holder discovery for AdapterStablecoinBalances (saved on Dune, query_id in adapter_stables_balances.py).
-- Per mapped stablecoin: the top_n addresses by amount received in a window of `days` days ending `offset_days` days ago
-- (offset_days = 0: the last `days` days; yearly windows with increasing offsets cover the full history without timeouts).
-- Used for discovery only; balances themselves are always read via balanceOf().
-- Params (text): blockchain (Dune alias, e.g. 'ethereum'), token_addresses (comma separated 0x...), days, offset_days, top_n
WITH tokens AS (
    SELECT from_hex(substr(trim(t), 3)) AS token
    FROM UNNEST(split('{{token_addresses}}', ',')) AS u(t)
),
inflow AS (
    SELECT
        tr.contract_address AS token_address,
        tr."to" AS address,
        SUM(CAST(tr.value AS double)) AS received_raw,
        COUNT(*) AS transfers
    FROM erc20_{{blockchain}}.evt_Transfer tr
    JOIN tokens tk ON tk.token = tr.contract_address
    WHERE tr.evt_block_date >= current_date - interval '{{offset_days}}' day - interval '{{days}}' day
      AND tr.evt_block_date < current_date - interval '{{offset_days}}' day + interval '1' day
      AND tr."to" != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY token_address ORDER BY received_raw DESC) AS rn
    FROM inflow
)
SELECT address, token_address, received_raw, transfers, rn
FROM ranked
WHERE rn <= {{top_n}}
