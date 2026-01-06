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
    TRY_CAST(modifieddate AS TIMESTAMP) AS modified_ts,
    row_hash,
    pk_hash,
    effective_from,
    effective_to,
    is_current,
    is_deleted,
    load_ts
FROM {{ source('raw_dim', 'person_person_history') }}
