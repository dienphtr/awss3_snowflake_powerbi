{{ config(materialized='table') }}

WITH header AS (
    SELECT *
    FROM {{ ref('fact_salesorderheader') }}
),

detail AS (
    SELECT *
    FROM {{ ref('fact_salesorderdetail') }}
),

product AS (
    SELECT *
    FROM {{ ref('dim_production_production') }}
),

customer AS (
    SELECT *
    FROM {{ ref('dim_customer') }}
),

territory AS (
    SELECT *
    FROM {{ ref('fact_salesterritory') }}
),

joined AS (
    SELECT
        -- Keys
        h.salesorder_id,
        d.salesorderdetail_id,
        d.productid,
        h.customerid,
        h.territoryid,

        -- Order header info
        h.order_date,
        h.due_date,
        h.ship_date,
        h.status,
        h.online_order_flag,
        h.salesordernumber,
        h.purchaseordernumber,
        h.account_number,
        h.salespersonid,
        h.billtoaddressid,
        h.shiptoaddressid,
        h.shipmethodid,
        h.creditcardid,
        h.creditcardapprovalcode,
        h.currencyrateid,
        h.subtotal,
        h.tax_amt,
        h.freight,
        h.total_due,
        h.comment,

        -- Order detail info
        d.order_qty,
        d.unit_price,
        d.unit_price_discount,
        d.line_total,

        -- Product info
        p.product_name,
        p.productnumber,
        p.standard_cost,
        p.list_price,
        p.color_clean,
        p.productline,
        p.class,
        p.style,
        p.productsubcategoryid,
        p.productmodelid,

        -- Customer info
        c.firstname,
        c.lastname,
        c.persontype,
        c.emailpromotion,
        c.storeid,

        -- Territory info
        t.territory_name,
        t.country_region_code,
        t.territory_group,

        -- Audit
        h.modified_ts AS header_modified_ts,
        d.modified_ts AS detail_modified_ts,
        c.modified_ts AS customer_modified_ts,
        p.modifieddate_ts AS product_modified_ts,
        t.modified_ts AS territory_modified_ts

    FROM header h
    INNER JOIN detail d
        ON h.salesorder_id = d.salesorder_id
    LEFT JOIN product p
        ON d.productid = p.product_id
    LEFT JOIN customer c
        ON h.customerid = c.customer_id
    LEFT JOIN territory t
        ON h.territoryid = t.territory_id
)

SELECT * FROM joined
