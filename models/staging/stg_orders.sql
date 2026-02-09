with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        order_id,
        event_id,
        customer_id,
        employee_id,
        venue_id,
        cast(order_timestamp as timestamp)              as order_timestamp,
        trim(lower(order_status))                       as order_status,
        trim(lower(payment_method))                     as payment_method,
        cast(subtotal as decimal(10,2))                 as subtotal,
        cast(tax_amount as decimal(10,2))               as tax_amount,
        cast(discount_amount as decimal(10,2))          as discount_amount,
        cast(tip_amount as decimal(10,2))               as tip_amount,
        cast(total_amount as decimal(10,2))             as total_amount,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
