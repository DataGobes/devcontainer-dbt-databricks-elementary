{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}
      
with source as (
    select distinct
        trim(full_ph)							as full_ph,					
        trim(ph1_product_type)                  as ph1_product_type,           
        trim(ph2_product_area)                  as ph2_product_area,           
        trim(ph3_article_group)                 as ph3_article_group,          
        trim(ph4_product_profit_centre)        as ph4_product_profit_centre,
        trim(ph5_article_sub_group)             as ph5_article_sub_group,      
        trim(prod_hier_text_de)                 as prod_hier_text_de,          
        trim(prod_hier_text_en)                 as prod_hier_text_en,          
        trim(profit_centre)                     as profit_centre,              
        trim(reference_material_no)             as reference_material_no,      
        trim(profit_centre_text_de)             as profit_centre_text_de,      
        trim(profit_centre_text_en)             as profit_centre_text_en,      
        trim(reference_prod_hierarchy)          as reference_prod_hierarchy,   
        trim(reporting_prod_category)           as reporting_prod_category,    
        trim(reference_prod_hier_text_de)       as reference_prod_hier_text_de,
        trim(reference_prod_hier_text_en)       as reference_prod_hier_text_en,
        trim(reporting_prod_grp1)               as reporting_prod_grp1,        
        trim(reporting_prod_grp1_text_de)       as reporting_prod_grp1_text_de,
        trim(reporting_prod_grp1_text_en)       as reporting_prod_grp1_text_en,
        trim(reporting_prod_grp2)               as reporting_prod_grp2,        
        trim(reporting_prod_grp2_text_de)       as reporting_prod_grp2_text_de,
        trim(reporting_prod_grp2_text_en)       as reporting_prod_grp2_text_en,
        trim(reporting_prod_grp3)               as reporting_prod_grp3,        
        trim(reporting_prod_grp3_text_de)       as reporting_prod_grp3_text_de,
        trim(reporting_prod_grp3_text_en)       as reporting_prod_grp3_text_en,
        trim(reporting_prod_grp4)               as reporting_prod_grp4,        
        trim(reporting_prod_grp4_text_de)       as reporting_prod_grp4_text_de,
        trim(reporting_prod_grp4_text_en)       as reporting_prod_grp4_text_en
    from {{ ref('sap_bw_controlling_product') }}
),

ph1_product_type as (

    select distinct full_ph, ph1_product_type, prod_hier_text_en 
    from source
    where ph2_product_area is null and ph3_article_group is null and ph4_product_profit_centre is null and ph5_article_sub_group is null

),

ph2_product_area as (

    select distinct full_ph, ph1_product_type, ph2_product_area, prod_hier_text_en 
    from source 
    where ph2_product_area is not null and ph3_article_group is null and ph4_product_profit_centre is null and ph5_article_sub_group is null

),

ph3_article_group as (

    select distinct full_ph, ph1_product_type, ph2_product_area, ph3_article_group, prod_hier_text_en 
    from source
    where ph3_article_group is not null and ph4_product_profit_centre is null and ph5_article_sub_group is null

),

ph4_product_profit_centre as (

    select distinct full_ph, ph1_product_type, ph2_product_area, ph3_article_group,ph4_product_profit_centre, prod_hier_text_en 
    from source
    where ph4_product_profit_centre is not null and ph5_article_sub_group is null

),

final as (
  select 
      ph5.full_ph,
      ph1.ph1_product_type,
      ph1.prod_hier_text_en as ph1_product_type_name,
      ph2.ph2_product_area,
      ph2.prod_hier_text_en as ph2_product_area_name,
      ph3.ph3_article_group,
      ph3.prod_hier_text_en as ph3_article_group_name,
      ph4.ph4_product_profit_centre,
      ph4.prod_hier_text_en as ph4_product_profit_centre_name,
      ph5.ph5_article_sub_group,
      case when ph5.ph5_article_sub_group is null then null else ph5.prod_hier_text_en end as ph5_article_sub_group_name,
      ph5.prod_hier_text_de,          
      ph5.prod_hier_text_en as prod_hier_name,          
      ph5.profit_centre,              
      ph5.reference_material_no,      
      ph5.profit_centre_text_de,      
      ph5.profit_centre_text_en as profit_centre_name,      
      ph5.reference_prod_hierarchy,   
      ph5.reporting_prod_category,    
      ph5.reference_prod_hier_text_de,
      ph5.reference_prod_hier_text_en as reporting_prod_category_name,
      ph5.reporting_prod_grp1,        
      ph5.reporting_prod_grp1_text_de,
      ph5.reporting_prod_grp1_text_en as reporting_prod_grp1_name,
      ph5.reporting_prod_grp2,        
      ph5.reporting_prod_grp2_text_de,
      ph5.reporting_prod_grp2_text_en as reporting_prod_grp2_name,
      ph5.reporting_prod_grp3,        
      ph5.reporting_prod_grp3_text_de,
      ph5.reporting_prod_grp3_text_en as reporting_prod_grp3_name,
      ph5.reporting_prod_grp4,        
      ph5.reporting_prod_grp4_text_de,
      ph5.reporting_prod_grp4_text_en as reporting_prod_grp4_name
  from source ph5
  left join ph1_product_type ph1 
    on ph5.ph1_product_type = ph1.ph1_product_type
  left join ph2_product_area ph2 
    on ph5.ph2_product_area  = ph2.ph2_product_area
    and ph5.ph1_product_type = ph2.ph1_product_type
  left join ph3_article_group ph3 
    on ph5.ph3_article_group  = ph3.ph3_article_group
    and ph5.ph1_product_type  = ph3.ph1_product_type
    and ph5.ph2_product_area  = ph3.ph2_product_area
  left join ph4_product_profit_centre ph4 
    on ph5.ph4_product_profit_centre  = ph4.ph4_product_profit_centre
    and ph5.ph1_product_type          = ph4.ph1_product_type
    and ph5.ph2_product_area          = ph4.ph2_product_area
    and ph5.ph3_article_group         = ph4.ph3_article_group
  order by full_ph
)

select *, 
        current_timestamp() as meta_insert_ts  from final

