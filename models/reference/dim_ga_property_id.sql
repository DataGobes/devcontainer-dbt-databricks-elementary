with properties as (
    select 
        *,  
        current_timestamp() as meta_insert_ts    
    from {{ ref('ga_properties') }}
)

select * from properties
