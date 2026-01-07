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
        TO_TIMESTAMP(SELLSTARTDATE, 'DD-Mon-YY HH:MI:SS AM') AS SELLSTARTDATE_TS,
        TO_TIMESTAMP(SELLENDDATE, 'DD-Mon-YY HH:MI:SS AM') AS SELLENDDATE_TS,
        DISCONTINUEDDATE,
        ROWGUID,
        TO_TIMESTAMP(MODIFIEDDATE, 'DD-Mon-YY HH:MI:SS AM') as modifieddate_ts,
        ROW_HASH,
        PK_HASH,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        IS_CURRENT,
        IS_DELETED,
        LOAD_TS
    FROM source
    WHERE IS_CURRENT = TRUE

)

SELECT * 
FROM renamed
