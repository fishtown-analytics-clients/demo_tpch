with line_item as (

    select * from {{ ref('order_item') }}

),

final as (

    select
        return_flag,
        order_item_status_code,
        sum(quantity) as sum_qty,
        sum(extended_price) as sum_base_price,
        sum(extended_price*(1-discount_percentage)) as sum_disc_price,
        sum(extended_price*(1-discount_percentage)*(1+tax_rate))as sum_charge,
        avg(quantity) as avg_price,
        avg(discount_percentage) as avg_disc,
        count(*) as count_order

    from 
        line_item
    where 
        ship_date <= dateadd('day', -3, '1998-12-01')
    group by
        return_flag,
        order_item_status_code
    order by
        return_flag,
        order_item_status_code
)

select * from final