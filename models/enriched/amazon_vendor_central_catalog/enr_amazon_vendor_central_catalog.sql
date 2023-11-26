with amazon_vendors as (

    select *
    from {{ ref('enr_amazon_vendors') }}

),

catalog_source as (

    select
        {{ dbt_utils.generate_surrogate_key(['v.amazon_country', 'c.asin_code']) }} as amazon_product_key,
        v.amazon_country,
        c.*
    from
        {{ source('src_amazon_vendor_central_catalog', 'amazon_vendor_central_catalog') }} as c
    left join
        amazon_vendors as v
        on c.vendor = v.vendor

),

-- Select all rows that come from manufacturing catalog files
manufacturing as (

    select distinct *
    from
        catalog_source
    where
        catalog_type = 'manufacturing'

),

-- Select all rows that come from sourcing catalog files and are not present in manufacturing
sourcing_not_in_manufacturing as (

    select distinct * --noqa:AM04
    from
        catalog_source
    where
        catalog_type = 'sourcing' and amazon_product_key not in (select amazon_product_key from manufacturing)
),

-- Union to get unique asins per vendor (still has duplicates because UK & UK2 are both UK when using amazon_country)
unique_asins_per_vendor as (

    select *
    from
        manufacturing
    union
    select *
    from
        sourcing_not_in_manufacturing
),

-- Get distincts (uniques)
unique_asins_per_country as (

    select distinct *
    from (
        select * except (vendor, meta_src, meta_insert_ts, meta_src_modification_ts)
        from
            unique_asins_per_vendor
    )
)

--select count(*) from unique_asins_per_country

--select amazon_product_key, amazon_country, collect_list(vendor), collect_list(product_title), collect_list(meta_src), count(*) from unique_asins_per_vendor group by amazon_product_key, amazon_country having count(*) > 1

-- Change metadata
select
    * except (catalog_type),
    current_timestamp() as meta_insert_ts
from unique_asins_per_country
