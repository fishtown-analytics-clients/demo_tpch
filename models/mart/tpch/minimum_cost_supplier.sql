    -- This query finds which supplier should be selected to place an order for a given part in a given region.

with supplier as (
    select * from {{ ref('stg_tpch__supplier') }}
),

nation as (
    select * from {{ ref('stg_tpch__nation') }}
),

part as (
    select * from {{ ref('stg_tpch__part')}}
),

region as (
    select * from {{ ref('stg_tpch__region') }}
),
part_supplier as (
    select * from {{ ref('stg_tpch__part_supply')}}
),

final as (
   select
        supplier.account_balance,
        supplier.name,
        nation.n_name,
        part.part_key,
        part.manufacturer,
        supplier.address,
        supplier.phone,
        supplier.comment
    from
        part,
        supplier,
        part_supplier,
        nation,
        region
    where
        part.part_key = part_supplier.part_key
        and supplier.supplier_key = part_supplier.supplier_key
        and part.size = '[SIZE]'
        and part.type like '%[TYPE]'
        and supplier.nation_key = nation.nation_key
        and nation.region_key = region.region_key
        and region.name = '[REGION]'
        and part_supplier.supply_cost = (
    select 
        min(part_supplier.supply_cost)
    from
        part_supplier, supplier,
        nation, region
    where
        part.part_key = part_supplier.part_key
        and supplier.supplier_key = part_supplier.supplier_key
        and supplier.nation_key = nation.nation_key
        and nation.region_key = region.region_key
        and region.name = '[REGION]'
    )
    order by
        supplier.account_balance desc,
        nation.n_name,
        supplier.name,
        part.part_key
)

select * from final