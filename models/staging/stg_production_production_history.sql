WITH source AS (

    SELECT * 
    FROM {{ source('raw_dim', 'production_product_history') }}

),

renamed AS (

    SELECT
        PRODUCTID,
        TRIM(NAME) AS PRODUCT_NAME,
        PRODUCTNUMBER,
        MAKEFLAG,
        FINISHEDGOODSFLAG,
        UPPER(TRIM(COLOR)) AS COLOR_CLEAN,
        SAFETYSTOCKLEVEL,
        REORDERPOINT,
        STANDARDCOST,
        LISTPRICE,
        SIZE,
        SIZEUNITMEASURECODE,
        WEIGHTUNITMEASURECODE,
        WEIGHT,
        DAYSTOMANUFACTURE,
        PRODUCTLINE,
        CLASS,
        STYLE,
        PRODUCTSUBCATEGORYID,
        PRODUCTMODELID,
        TRY_CAST(SELLSTARTDATE AS TIMESTAMP) AS SELLSTARTDATE_TS,
        TRY_CAST(SELLENDDATE AS TIMESTAMP) AS SELLENDDATE_TS,
        DISCONTINUEDDATE,
        ROWGUID,
        TRY_CAST(MODIFIEDDATE AS TIMESTAMP) AS MODIFIEDDATE_TS,
        ROW_HASH,
        PK_HASH,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        IS_CURRENT,
        IS_DELETED,
        LOAD_TS
    FROM source

)

SELECT * 
FROM renamed
