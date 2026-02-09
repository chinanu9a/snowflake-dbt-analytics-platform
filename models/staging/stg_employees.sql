with source as (

    select * from {{ source('raw', 'raw_employees') }}

),

renamed as (

    select
        employee_id,
        trim(first_name)                                as first_name,
        trim(last_name)                                 as last_name,
        trim(lower(role))                               as role,
        venue_id,
        cast(hire_date as date)                         as hire_date,
        cast(hourly_rate as decimal(10,2))              as hourly_rate,
        cast(is_active as boolean)                      as is_active,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
