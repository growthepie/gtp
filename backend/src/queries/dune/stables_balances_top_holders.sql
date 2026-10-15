-- Top-holder discovery for AdapterStablecoinBalances (saved on Dune, query_id in adapter_stables_balances.py). Run once per chain as a seed.
-- Per stablecoin: the top_n addresses by approximate balance (all-time transfers in minus out).
-- The approximation is only used to rank addresses; balances themselves are always read via balanceOf().
-- Params (text): blockchain (Dune alias, e.g. 'ethereum'), token_addresses (comma separated 0x...), top_n
WITH tokens AS (
    SELECT from_hex(substr(trim(t), 3)) AS token
    FROM UNNEST(split('{{token_addresses}}', ',')) AS u(t)
),
flows AS (
    SELECT tr.contract_address AS token_address, tr."to" AS address, CAST(tr.value AS double) AS v
    FROM erc20_{{blockchain}}.evt_Transfer tr
    JOIN tokens tk ON tk.token = tr.contract_address
    UNION ALL
    SELECT tr.contract_address, tr."from", -CAST(tr.value AS double)
    FROM erc20_{{blockchain}}.evt_Transfer tr
    JOIN tokens tk ON tk.token = tr.contract_address
),
balances AS (
    SELECT token_address, address, SUM(v) AS balance_raw
    FROM flows
    WHERE address != 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
    HAVING SUM(v) > 0
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY token_address ORDER BY balance_raw DESC) AS rn
    FROM balances
)
SELECT address, token_address, balance_raw, rn
FROM ranked
WHERE rn <= {{top_n}}
