{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        TERRITORYID AS territory_id,
        COALESCE(territory_name, 'UNKNOWN') AS territory_name,
        country_region_code,
        COALESCE(territory_group, 'UNKNOWN') AS territory_group,
        COALESCE(SALESYTD, 0) AS sales_ytd,
        COALESCE(SALESLASTYEAR, 0) AS sales_last_year,
        COALESCE(COSTYTD, 0) AS cost_ytd,
        COALESCE(COSTLASTYEAR, 0) AS cost_last_year,
        ROWGUID,
        modified_ts,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY TERRITORYID
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_salesterritory_history') }}
    WHERE TERRITORYID IS NOT NULL
)

SELECT
    territory_id,
    territory_name,
    country_region_code,
    territory_group,
    sales_ytd,
    sales_last_year,
    cost_ytd,
    cost_last_year,
    ROWGUID,
    modified_ts
FROM ranked
WHERE rn = 1
