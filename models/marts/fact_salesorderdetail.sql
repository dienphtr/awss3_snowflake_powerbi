{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        SALESORDERID AS salesorder_id,
        SALESORDERDETAILID AS salesorderdetail_id,
        PRODUCTID,
        SPECIALOFFERID,
        COALESCE(ORDERQTY, 0) AS order_qty,
        COALESCE(UNITPRICE, 0) AS unit_price,
        COALESCE(UNITPRICEDISCOUNT, 0) AS unit_price_discount,
        COALESCE(LINETOTAL, order_qty * unit_price * (1 - unit_price_discount)) AS line_total,
        ROWGUID,
        modified_ts,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY SALESORDERDETAILID
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_salesorderdetail_history') }}
    WHERE SALESORDERDETAILID IS NOT NULL
)

SELECT
    salesorder_id,
    salesorderdetail_id,
    PRODUCTID,
    SPECIALOFFERID,
    order_qty,
    unit_price,
    unit_price_discount,
    line_total,
    ROWGUID,
    modified_ts
FROM ranked
WHERE rn = 1
