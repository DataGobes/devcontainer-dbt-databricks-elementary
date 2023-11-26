with source as (
    select * from {{ ref('dim_product_master_controlling') }}
),

renamed as (
    select 
        dim_product_master_id                   as `Dim Product Master Id`,
        product_hierarchy                       as `ProductHierarchy`,
        ph1_product_type                        as `Ph1 Product Type`,
        ph1_product_type_name                   as `Ph1 Product Type Name`,
        ph2_product_area                        as `Ph2 Product Area`,
        ph2_product_area_name                   as `Ph2 Product Area Name`,
        ph3_article_group                       as `Ph3 Article Group`,
        ph3_article_group_name                  as `Ph3 Article Group Name`,
        ph4_product_profit_centre               as `Ph4 Product Profit Centre`,
        ph4_product_profit_centre_name          as `Ph4 Product Profit Centre Name`,
        ph5_article_sub_group                   as `Ph5 Article Sub Group`,
        ph5_article_sub_group_name              as `Ph5 Article Sub Group Name`,
        profit_centre                           as `Profit Centre`,
        reference_material_no                   as `Reference Material No`,
        profit_centre_name                      as `Profit Centre Name`,
        reference_prod_hierarchy                as `Reference Prod Hierarchy`,
        reporting_prod_category                 as `Reporting Prod Category`,
        reporting_prod_category_name            as `Reporting Prod Category Name`,
        reporting_prod_grp1                     as `Reporting Prod Grp1`,
        reporting_prod_grp1_name                as `Reporting Prod Grp1 Name`,
        reporting_prod_grp2                     as `Reporting Prod Grp2`,
        reporting_prod_grp2_name                as `Reporting Prod Grp2 Name`,
        reporting_prod_grp3                     as `Reporting Prod Grp3`,
        reporting_prod_grp3_name                as `Reporting Prod Grp3 Name`,
        reporting_prod_grp4                     as `Reporting Prod Grp4`,
        reporting_prod_grp4_name                as `Reporting Prod Grp4 Name`
from source
)

select * from renamed