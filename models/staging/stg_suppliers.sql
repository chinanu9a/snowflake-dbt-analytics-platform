with source as (

    select * from {{ source('raw', 'raw_suppliers') }}

),

renamed as (

    select
        supplier_id,
        trim(supplier_name)                             as supplier_name,
        trim(contact_name)                              as contact_name,
        lower(trim(contact_email))                      as contact_email,
        trim(contact_phone)                             as contact_phone,
        trim(lower(category))                           as category,
        trim(city)                                      as city,
        upper(trim(state))                              as state,
        cast(is_active as boolean)                      as is_active,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
