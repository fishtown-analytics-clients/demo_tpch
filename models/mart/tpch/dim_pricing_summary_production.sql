with line_item as (

    select * from {{ ref('stg_tpch__line_item') }}

),

final as (

    select
        return_flag,
        line_status,
        sum(quantity) as sum_qty,
        sum(extended_price) as sum_base_price,
        sum(extended_price*(1-discount)) as sum_disc_price,
        sum(extended_price*(1-discount)*(1+tax))as sum_charge,
        avg(quantity) as avg_price,
        avg(discount) as avg_disc,
        count(*) as count_order

    from 
        line_item
    where 
        ship_date <= dateadd('day', -3, '1998-12-01')
    group by
        return_flag,
        line_status
    order by
        return_flag,
        line_status
)

select * from final