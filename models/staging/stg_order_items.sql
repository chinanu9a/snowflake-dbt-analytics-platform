with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        menu_item_id,
        cast(quantity as integer)                       as quantity,
        cast(unit_price as decimal(10,2))               as unit_price,
        cast(line_discount as decimal(10,2))            as line_discount,
        cast(line_tax as decimal(10,2))                 as line_tax,
        cast(line_total as decimal(10,2))               as line_total,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
