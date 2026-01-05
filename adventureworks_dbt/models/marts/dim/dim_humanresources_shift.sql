SELECT
  SHIFTID                         AS shift_id,
  NAME                            AS shift_name,
  TO_TIME(STARTTIME)              AS start_time,
  TO_TIME(ENDTIME)                AS end_time,
  TO_TIMESTAMP(
    MODIFIEDDATE,
    'DD-MON-YY HH:MI:SS AM'
  )                               AS modified_at
FROM {{ ref('stg_humanresources_shift') }}
