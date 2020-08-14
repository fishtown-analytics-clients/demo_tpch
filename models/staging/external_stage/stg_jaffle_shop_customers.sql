with source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

select
    id,
    first_name,
    last_name

from source
