{{ config(materialized='view') }}

SELECT
    TERRITORYID,
    TRIM(NAME) AS territory_name,
    UPPER(TRIM(COUNTRYREGIONCODE)) AS country_region_code,
    TRIM("GROUP") AS territory_group,
    SALESYTD,
    SALESLASTYEAR,
    COSTYTD,
    COSTLASTYEAR,
    ROWGUID,
    TO_TIMESTAMP(MODIFIEDDATE, 'DD-Mon-YY HH:MI:SS AM') as modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_salesterritory_history') }}
WHERE IS_CURRENT = TRUE