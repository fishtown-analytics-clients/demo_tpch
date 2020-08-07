-- This query retrieves the 10 unshipped orders with the highest value.

with customer as (
    
    select * from {{ ref('stg_tpch__customer') }}
),

line_item as (
    
    select * from {{ ref('stg_tpch__line_item') }}
),

orders as (
    select * from {{ ref('stg_tpch__orders')}}
),

final as (

    select
        line_item.order_key,
        sum(line_item.extended_price*(1-line_item.discount)) as revenue,
        orders.order_date,
        orders.ship_priority
    from
        customer,
        orders,
        line_item
    where
        customer.market_segment = '[SEGMENT]'
        and customer.customer_key = orders.customer_key
        and line_item.order_key = orders.order_key
        and orders.order_date < date '[DATE]'
        and line_item.ship_date > date '[DATE]'
    group by
        line_item.order_key,
        orders.order_date,
        orders.ship_priority
    order by
        revenue desc,
        orders.order_date
    )

select * from final