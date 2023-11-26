{{ config(
    schema = "dwh",
    materialized = "table",
    format = "delta"  
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_product_master_controlling') }}
),

pim_product_master as (
    select * from {{ source('src_dim_product_master', 'dim_product_master_pim') }} 
)

select distinct
    dim_product_master_id,
    product_hierarchy,
    coalesce(ph1_product_type,              'Not Set') as  ph1_product_type,                    
    coalesce(ph1_product_type_name,         'Not Set') as  ph1_product_type_name,       
    coalesce(ph2_product_area,              'Not Set') as  ph2_product_area,         
    coalesce(ph2_product_area_name,         'Not Set') as  ph2_product_area_name,        
    coalesce(ph3_article_group,             'Not Set') as  ph3_article_group,                 
    coalesce(ph3_article_group_name,        'Not Set') as  ph3_article_group_name,                
    coalesce(ph4_product_profit_centre,     'Not Set') as  ph4_product_profit_centre,                
    coalesce(ph4_product_profit_centre_name,'Not Set') as  ph4_product_profit_centre_name,                
    coalesce(ph5_article_sub_group,         'Not Set') as  ph5_article_sub_group,               
    coalesce(ph5_article_sub_group_name,    'Not Set') as  ph5_article_sub_group_name,               
    coalesce(profit_centre,                 'Not Set') as  profit_centre,                
    coalesce(reference_material_no,         'Not Set') as  reference_material_no,               
    coalesce(profit_centre_name,            'Not Set') as  profit_centre_name,               
    coalesce(reference_prod_hierarchy,      'Not Set') as  reference_prod_hierarchy,               
    coalesce(reporting_prod_category,       'Not Set') as  reporting_prod_category,                 
    coalesce(reporting_prod_category_name,  'Not Set') as  reporting_prod_category_name,                 
    coalesce(reporting_prod_grp1,           'Not Set') as  reporting_prod_grp1,                 
    coalesce(reporting_prod_grp1_name,      'Not Set') as  reporting_prod_grp1_name,                 
    coalesce(reporting_prod_grp2,           'Not Set') as  reporting_prod_grp2,                 
    coalesce(reporting_prod_grp2_name,      'Not Set') as  reporting_prod_grp2_name,                 
    coalesce(reporting_prod_grp3,           'Not Set') as  reporting_prod_grp3,                 
    coalesce(reporting_prod_grp3_name,      'Not Set') as  reporting_prod_grp3_name,                 
    coalesce(reporting_prod_grp4,           'Not Set') as  reporting_prod_grp4,                 
    coalesce(reporting_prod_grp4_name,      'Not Set') as  reporting_prod_grp4_name,                 
    current_timestamp()                                as meta_insert_ts  
from pim_product_master m
left join source s
  on m.product_hierarchy = s.full_ph
order by 1