{{ config(materialized='table') }}

WITH customer AS (
    SELECT *
    FROM {{ ref('fact_sales_customer') }}
),

person AS (
    SELECT *
    FROM {{ ref('dim_person') }}
),

joined AS (
    SELECT
        c.customer_id,
        c.account_number,
        c.territoryid,
        c.storeid,
        p.firstname,
        p.lastname,
        p.persontype,
        p.emailpromotion,
        c.modified_ts
    FROM customer c
    LEFT JOIN person p
        ON c.personid = p.customer_id
)

SELECT * FROM joined
