{{ config(materialized='view') }}

SELECT
    businessentityid,
    persontype,
    namestyle,
    title,
    firstname,
    middlename,
    lastname,
    suffix,
    emailpromotion,
    additionalcontactinfo,
    demographics,
    rowguid,
    TO_TIMESTAMP(MODIFIEDDATE, 'DD-Mon-YY HH:MI:SS AM') as modified_ts,
    row_hash,
    pk_hash,
    effective_from,
    effective_to,
    is_current,
    is_deleted,
    load_ts
FROM {{ source('raw_dim', 'person_person_history') }}
