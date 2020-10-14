{{
    config(
        materialized = 'table'
    )
}}

with 

parts as (

    select * from {{ref('stg_tpch__parts')}}

),

final as (

    select 
        part_key,
        name,
        manufacturer,
        brand,
        type,
        size,
        container,
        retail_price
    from parts

)

select * from final