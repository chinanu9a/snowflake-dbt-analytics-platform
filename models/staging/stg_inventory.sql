with source as (

    select * from {{ source('raw', 'raw_inventory') }}

),

renamed as (

    select
        inventory_id,
        venue_id,
        supplier_id,
        trim(item_name)                                 as item_name,
        trim(lower(category))                           as category,
        trim(lower(unit_of_measure))                    as unit_of_measure,
        cast(quantity_on_hand as integer)                as quantity_on_hand,
        cast(reorder_point as integer)                  as reorder_point,
        cast(unit_cost as decimal(10,2))                as unit_cost,
        cast(last_restock_date as date)                 as last_restock_date,
        cast(expiration_date as date)                   as expiration_date,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at

    from source

)

select * from renamed
