with supplier as (

    select * from {{ ref('stg_tpch__supplier') }}

),

with part as (
    
    select * from {{ ref('stg_tpch__part') }}
),

with nation as (
    
    select * from {{ ref('stg_tpch__nation') }}
),

with part_supplier as (
    
    select * from {{ ref('stg_tpch__part_supply') }}
),

with region as (
    
    select * from {{ ref('stg_tpch__region') }}
)

final as (
    select 
        supplier.account_balance,
        supplier.name,
        nation.name,
        part.part_key,
        pars.manaufacture,
        supplier.address,
        supplier.phone,
        supplier.comment

    from 
        part,
        supplier,
        partsupp,
        nation,
        region

    where 
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = [SIZE]
        and p_type like '%[TYPE]'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '[REGION]'
        and ps_supplycost = (
                select
                    min(ps_supplycost)
                from
                    partsupp, supplier,
                    nation, region
                where
                    p_partkey = ps_partkey
                    and s_suppkey = ps_suppkey
                    and s_nationkey = n_nationkey
                    and n_regionkey = r_regionkey
                    and r_name = '[REGION]'
)

order by
s_acctbal desc,
n_name,
s_name,
p_partkey;
)
