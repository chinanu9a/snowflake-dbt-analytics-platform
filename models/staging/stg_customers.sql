with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        trim(first_name)                                as first_name,
        trim(last_name)                                 as last_name,
        lower(trim(email))                              as email,
        trim(phone)                                     as phone,
        lower(trim(loyalty_tier))                       as loyalty_tier,
        cast(loyalty_points as integer)                 as loyalty_points,
        cast(signup_date as date)                       as signup_date,
        cast(birth_date as date)                        as birth_date,
        trim(city)                                      as city,
        upper(trim(state))                              as state,
        cast(is_active as boolean)                      as is_active,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
