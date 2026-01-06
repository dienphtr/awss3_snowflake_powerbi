{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        SPECIALOFFERID AS specialoffer_id,
        COALESCE(description, 'UNKNOWN') AS description,
        COALESCE(DISCOUNTPCT, 0) AS discount_pct,
        UPPER(TRIM(TYPE)) AS offer_type,
        UPPER(TRIM(CATEGORY)) AS offer_category,
        start_date,
        end_date,
        COALESCE(MINQTY, 0) AS min_qty,
        COALESCE(MAXQTY, 999999) AS max_qty,
        ROWGUID,
        modified_ts,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY SPECIALOFFERID
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_sales_specialoffer_history') }}
    WHERE SPECIALOFFERID IS NOT NULL
)

SELECT
    specialoffer_id,
    description,
    discount_pct,
    offer_type,
    offer_category,
    start_date,
    end_date,
    min_qty,
    max_qty,
    ROWGUID,
    modified_ts
FROM ranked
WHERE rn = 1
