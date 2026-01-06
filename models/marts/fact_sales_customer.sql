{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        CUSTOMERID AS customer_id,
        PERSONID,
        STOREID,
        TERRITORYID,
        COALESCE(account_number, 'UNKNOWN') AS account_number,
        ROWGUID,
        modified_ts,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMERID
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_sales_customer_history') }}
    WHERE CUSTOMERID IS NOT NULL
)

SELECT
    customer_id,
    PERSONID,
    STOREID,
    TERRITORYID,
    account_number,
    ROWGUID,
    modified_ts
FROM ranked
WHERE rn = 1
