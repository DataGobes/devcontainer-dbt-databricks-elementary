with dwh_dim_amazon_asin_master as (

    select *
    from
        {{ ref('dwh_dim_amazon_asin_master') }}
),

renamed as (

    select
        amazon_product_key as `Amazon Product Key`,
        asin_code as `ASIN Code`,
        amazon_country as `Amazon Country`,
        product_title as `Product Title`,
        manufacturer_code as `Manufacturer Code`,
        parent_asin as `Parent ASIN`,
        upc as `UPC Code`,
        ean as `EAN Code`,
        isbn_13 as `ISBN-13`,
        model_number as `Model Number`,
        brand as `Brand`,
        brand_code as `Brand Code`,
        product_group as `Product Group`,
        release_date as `Release Date`,
        replenishment_category as `Replenishment Category`,
        prep_instructions_required as `Prep Instructions Required`,
        prep_instructions_vendor_state as `rep Instructions Vendor State`
    from dwh_dim_amazon_asin_master

)

select *
from renamed
