with source as (

    select * from {{ source('raw', 'raw_menu_items') }}

),

renamed as (

    select
        menu_item_id,
        trim(item_name)                                 as item_name,
        trim(lower(category))                           as category,
        trim(lower(subcategory))                        as subcategory,
        cast(unit_price as decimal(10,2))               as unit_price,
        cast(cost_price as decimal(10,2))               as cost_price,
        cast(is_alcoholic as boolean)                   as is_alcoholic,
        cast(is_available as boolean)                   as is_available,
        venue_id,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
