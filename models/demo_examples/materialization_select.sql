with materialization_ephemeral as 
    (select * from {{ref('materialization_ephemeral')}}
,materialization_view as 
    (select * from {{ref('materialization_view')}}
,materialization_table as 
    (select * from {{ref('materialization_table')}}

select * from materialization_ephemeral

union all 

select * from materialization_view

union all

select * from materialization_table