SELECT *
FROM {{ ref('fact_sales_specialoffer') }}
WHERE discount_pct < 0
  AND discount_pct IS NOT NULL
