{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        PRODUCTID AS product_id,
        COALESCE(product_name, 'UNKNOWN') AS product_name,
        PRODUCTNUMBER,
        COALESCE(STANDARDCOST, 0) AS standard_cost,
        COALESCE(LISTPRICE, 0) AS list_price,
        COLOR_CLEAN,
        SAFETYSTOCKLEVEL,
        REORDERPOINT,
        WEIGHT,
        PRODUCTLINE,
        CLASS,
        STYLE,
        PRODUCTSUBCATEGORYID,
        PRODUCTMODELID,
        SELLSTARTDATE_TS,
        SELLENDDATE_TS,
        MODIFIEDDATE_TS,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCTID
            ORDER BY modifieddate_ts DESC
        ) AS rn
    FROM {{ ref('stg_production_production_history') }}
    WHERE PRODUCTID IS NOT NULL
)

SELECT
    product_id,
    product_name,
    PRODUCTNUMBER,
    standard_cost,
    list_price,
    COLOR_CLEAN,
    SAFETYSTOCKLEVEL,
    REORDERPOINT,
    WEIGHT,
    PRODUCTLINE,
    CLASS,
    STYLE,
    PRODUCTSUBCATEGORYID,
    PRODUCTMODELID,
    sellstartdate_ts,
    sellenddate_ts,
    modifieddate_ts
FROM ranked
WHERE rn = 1
