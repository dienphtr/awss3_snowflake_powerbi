{{ config(materialized='view') }}

SELECT
    SPECIALOFFERID,
    TRIM(DESCRIPTION) AS description,
    DISCOUNTPCT,
    TYPE,
    CATEGORY,
    TO_TIMESTAMP(STARTDATE, 'DD-Mon-YY HH:MI:SS AM') AS start_date,
    TO_TIMESTAMP(ENDDATE, 'DD-Mon-YY HH:MI:SS AM') AS end_date,
    MINQTY,
    MAXQTY,
    ROWGUID,
    TO_TIMESTAMP(MODIFIEDDATE, 'DD-Mon-YY HH:MI:SS AM') as modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_specialoffer_history') }}
WHERE IS_CURRENT = TRUE 
