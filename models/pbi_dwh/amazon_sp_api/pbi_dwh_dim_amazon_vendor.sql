select
    vendor,
    vg,
    amazon_country as `Amazon Country`,
    vendor_code as `Vendor Code`,
    vendor_name as `Vendor Name`,
    calculate_in_sourcing
from {{ ref('dwh_dim_amazon_vendor') }}
