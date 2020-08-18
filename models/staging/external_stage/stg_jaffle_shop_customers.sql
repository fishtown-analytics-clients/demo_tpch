{{ config(
  enabled= false,
  tags = ["customers"]
) }}

with source as (

    select * from {{ source('jaffle_shop', 'customers') }}

)

select
    id,
    first_name,
    last_name,
    email

from source
