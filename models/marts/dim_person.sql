{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        businessentityid AS customer_id,
        firstname,
        lastname,
        persontype,
        emailpromotion,
        is_current,
        modified_ts,
        row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY businessentityid
            ORDER BY modified_ts DESC
        ) AS rn
    FROM {{ ref('stg_person_person_history') }}
    WHERE is_current = TRUE
      AND businessentityid IS NOT NULL
)

SELECT
    customer_id,
    firstname,
    lastname,
    persontype,
    emailpromotion
FROM ranked
WHERE rn = 1
