{% test positive_values_allow_null(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} <= 0
  AND {{ column_name }} IS NOT NULL

{% endtest %}
