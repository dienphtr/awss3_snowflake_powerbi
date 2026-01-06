{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        SALESORDERID AS salesorder_id,
        REVISIONNUMBER,
        order_date,
        due_date,
        ship_date,
        STATUS,
        CASE 
            WHEN ONLINEORDERFLAG IN ('1','Y','TRUE') THEN TRUE
            ELSE FALSE
        END AS online_order_flag,
        SALESORDERNUMBER,
        PURCHASEORDERNUMBER,
        COALESCE(ACCOUNTNUMBER, 'UNKNOWN') AS account_number,
        CUSTOMERID,
        SALESPERSONID,
        TERRITORYID,
        BILLTOADDRESSID,
        SHIPTOADDRESSID,
        SHIPMETHODID,
        CREDITCARDID,
        CREDITCARDAPPROVALCODE,
        CURRENCYRATEID,
        COALESCE(SUBTOTAL,0) AS subtotal,
        COALESCE(TAXAMT,0) AS tax_amt,
        COALESCE(FREIGHT,0) AS freight,
        COALESCE(TOTALDUE, subtotal+tax_amt+freight) AS total_due,
        COMMENT,
        ROWGUID,
        modified_ts,
        IS_CURRENT,
        IS_DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY SALESORDERID
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_salesorderheader_history') }}
    WHERE SALESORDERID IS NOT NULL
        AND order_date IS NOT NULL
)

SELECT
    salesorder_id,
    revisionnumber,
    order_date,
    due_date,
    ship_date,
    status,
    online_order_flag,
    salesordernumber,
    purchaseordernumber,
    account_number,
    customerid,
    salespersonid,
    territoryid,
    billtoaddressid,
    shiptoaddressid,
    shipmethodid,
    creditcardid,
    creditcardapprovalcode,
    currencyrateid,
    subtotal,
    tax_amt,
    freight,
    total_due,
    comment,
    rowguid,
    modified_ts
FROM ranked
WHERE rn = 1
