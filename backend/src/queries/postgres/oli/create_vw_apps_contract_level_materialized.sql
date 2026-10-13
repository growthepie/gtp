DROP MATERIALIZED VIEW IF EXISTS public.vw_apps_contract_level_materialized;

CREATE MATERIALIZED VIEW public.vw_apps_contract_level_materialized
TABLESPACE pg_default
AS WITH labeled_facts AS (
    SELECT
        fact.date,
        fact.address,
        fact.origin_key,
        oli.contract_name,
        oli.owner_project,
        oli.usage_category,
        fact.txcount,
        fact.gas_fees_eth,
        fact.gas_fees_usd,
        fact.daa,
        fact.success_rate
    FROM public.blockspace_fact_contract_level fact
    JOIN public.vw_oli_label_pool_gold_pivoted_cross_chain_v1 oli
        ON fact.address = oli.address
        AND fact.origin_key = oli.origin_key
    WHERE oli.owner_project IS NOT NULL

    UNION ALL

    -- 'eth' app: all native transfers, mapped to the placeholder address 0x000000000000000000000000000000000000EeEe (0xEeee...EEeE is labeled as 'exactly' in OLI)
    -- only chains with ETH as gas token have native transfer senders in fact_active_addresses_contract_hll (see aggregate_native_transfer_senders_hll)
    SELECT
        nt.date,
        decode('000000000000000000000000000000000000eeee', 'hex') AS address,
        nt.origin_key,
        'Native transfers' AS contract_name,
        'eth' AS owner_project,
        'native_transfer' AS usage_category,
        nt.txcount,
        nt.gas_fees_eth,
        nt.gas_fees_usd,
        nt.daa,
        1 AS success_rate -- not tracked for native transfers, these practically never revert
    FROM public.blockspace_fact_category_level nt
    WHERE nt.category_id = 'native_transfer'
        AND EXISTS (
            SELECT 1
            FROM public.fact_active_addresses_contract_hll hll
            WHERE hll.address = decode('000000000000000000000000000000000000eeee', 'hex')
                AND hll.origin_key = nt.origin_key
        )
)
SELECT
    labeled_facts.date,
    labeled_facts.address,
    labeled_facts.origin_key,
    labeled_facts.contract_name,
    labeled_facts.owner_project,
    cat.main_category_id AS main_category_key,
    labeled_facts.usage_category AS sub_category_key,
    sum(labeled_facts.txcount) AS txcount,
    sum(labeled_facts.gas_fees_eth) AS fees_paid_eth,
    sum(labeled_facts.gas_fees_usd) AS fees_paid_usd,
    sum(labeled_facts.daa) AS daa,
    sum(labeled_facts.success_rate) AS success_rate
FROM labeled_facts
LEFT JOIN public.oli_categories cat
    ON labeled_facts.usage_category = cat.category_id::text
GROUP BY
    labeled_facts.date,
    labeled_facts.address,
    labeled_facts.origin_key,
    labeled_facts.contract_name,
    labeled_facts.owner_project,
    cat.main_category_id,
    labeled_facts.usage_category
WITH DATA;

CREATE INDEX idx_date ON public.vw_apps_contract_level_materialized USING btree (date);
CREATE INDEX idx_origin_key ON public.vw_apps_contract_level_materialized USING btree (origin_key);
CREATE INDEX idx_owner_project ON public.vw_apps_contract_level_materialized USING btree (owner_project);
