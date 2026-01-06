{{ config(materialized='view') }}

SELECT
    SALESORDERID,
    SALESORDERDETAILID,
    CARRIERTRACKINGNUMBER,
    ORDERQTY,
    PRODUCTID,
    SPECIALOFFERID,
    UNITPRICE,
    UNITPRICEDISCOUNT,
    LINETOTAL,
    ROWGUID,
    TRY_CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_salesorderdetail_history') }}
