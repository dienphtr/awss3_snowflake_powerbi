with source as (

    select *
    from {{ source('adventureworks_raw', 'HUMANRESOURCES_SHIFT') }}

),

renamed as (

    select
        shiftid::integer          as shift_id,
        name::varchar(100)        as shift_name,

        -- time
        try_to_time(starttime)    as start_time,
        try_to_time(endtime)      as end_time,

        -- timestamp
        try_to_timestamp(modifieddate) as modified_at

    from source
)

select * from renamed
