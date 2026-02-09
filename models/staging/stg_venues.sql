with source as (

    select * from {{ source('raw', 'raw_venues') }}

),

renamed as (

    select
        venue_id,
        trim(lower(venue_name))                         as venue_name,
        trim(lower(venue_type))                         as venue_type,
        trim(city)                                      as city,
        upper(trim(state))                              as state,
        upper(trim(country))                            as country,
        cast(capacity as integer)                       as capacity,
        trim(timezone)                                  as timezone,
        cast(opened_date as date)                       as opened_date,
        cast(is_active as boolean)                      as is_active,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
