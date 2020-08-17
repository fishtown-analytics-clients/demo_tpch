{{ config(
  enabled= true,
  tags = ["orders"]
) }}

with source as (

    select * from {{ source('jaffle_shop', 'orders') }}

)

select
    id,
    user_id,
    order_date,
    status
from source