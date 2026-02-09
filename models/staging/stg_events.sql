with source as (

    select * from {{ source('raw', 'raw_events') }}

),

renamed as (

    select
        event_id,
        venue_id,
        trim(event_name)                                as event_name,
        trim(lower(event_type))                         as event_type,
        cast(event_date as date)                        as event_date,
        cast(event_start_time as time)                  as event_start_time,
        cast(event_end_time as time)                    as event_end_time,
        cast(expected_attendance as integer)             as expected_attendance,
        cast(actual_attendance as integer)               as actual_attendance,
        cast(is_cancelled as boolean)                   as is_cancelled,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
