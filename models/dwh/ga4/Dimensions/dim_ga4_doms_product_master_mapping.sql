with ga4_items as (
    select
        *,
        lpad(trim(item_id), 8, '0') as ga_product_id
    from {{ ref('enr_ga4_domestic_items') }}
),

miele_master as (
    select *
    from {{ source('src_dim_product_master','dim_product_master_pim') }}
),

join_master as (
    select distinct
        ga4_items.item_key,
        ga4_items.item_id as ga_item_id,
        'ga4' as product_id_source,
        coalesce(miele_master.dim_product_master_key, -1) as dim_product_master_key
    from ga4_items
    left outer join miele_master
        on ga4_items.ga_product_id = miele_master.dim_product_master_id
),

final as (
    select
        item_key,
        dim_product_master_key,
        ga_item_id,
        product_id_source,
        current_timestamp() as meta_insert_ts
    from join_master
)

select * from final
