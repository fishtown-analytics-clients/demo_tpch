{{ config(
  enabled= true,
  tags = ["payments"]
) }}

with source as (

    select * from {{ source('jaffle_shop', 'payments') }}

)

select
    id,
    order_id,
    payment_method,
    ammount

from source