{{ config(materialized='view') }}

SELECT
    SPECIALOFFERID,
    TRIM(DESCRIPTION) AS description,
    DISCOUNTPCT,
    TYPE,
    CATEGORY,
    TRY_CAST(STARTDATE AS TIMESTAMP) AS start_date,
    TRY_CAST(ENDDATE AS TIMESTAMP) AS end_date,
    MINQTY,
    MAXQTY,
    ROWGUID,
    TRY_CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_specialoffer_history') }}
