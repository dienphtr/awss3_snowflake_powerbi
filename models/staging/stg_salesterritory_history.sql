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
    TRY_CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_salesterritory_history') }}
