{{ config(materialized='view') }}

SELECT
    CUSTOMERID,
    PERSONID,
    STOREID,
    TERRITORYID,
    TRIM(ACCOUNTNUMBER) AS account_number,
    ROWGUID,
    TRY_CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_customer_history') }}
