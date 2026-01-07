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
    TO_TIMESTAMP(MODIFIEDDATE, 'DD-Mon-YY HH:MI:SS AM') as modified_ts,
    ROW_HASH,
    PK_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    IS_DELETED,
    LOAD_TS
FROM {{ source('raw_fact', 'sales_salesorderdetail_history') }}
WHERE IS_CURRENT = TRUE