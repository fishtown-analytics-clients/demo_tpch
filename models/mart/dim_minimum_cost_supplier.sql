with supplier as (
    
    select * from {{ ref('supplier') }}
),

part as (
    
    select * from {{ ref('part') }}
),

nation as (
    
    select * from {{ ref('nation') }}
),

part_supplier as (
    
    select * from {{ ref('part_supplier') }}
),

region as (
    
    select * from {{ ref('region') }}
),

final as (
    select 
        supplier.account_balance,
        supplier.name as supplier_name,
        nation.name as nation_name,
        part.part_key,
        part.manufacturer,
        supplier.address,
        supplier.phone_number,
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
        and part_supplier.cost = 
        (
            select
                min(part_supplier.cost)
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
    nation.name,
    supplier.name,
    part.part_key

)

select * from final