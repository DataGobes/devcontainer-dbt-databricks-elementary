{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}
      
-- take the source data only with the flag active Y (the ones with a negative active filter are history records that will only live in the raw table)
with source as (

    select *
    from
        {{ source('src_raw_restricted', 'sap_bw_sales_order_financials_next3') }}
    where is_active = 'Y'

),


source_enriched as (

    select
        cast(coalesce(`CALDAY`       ,19700101) as bigint)     as date_posting_copa,
        coalesce(`/BIC/GC_0001`     ,'Not Set')                as reporting_unit_code,
        coalesce(`/BIC/GC_PRCTYP`   ,'Not Set')                as crm_transaction_type_code,
        coalesce(`/BIC/GDISTR_CH`   ,'Not Set')                as distribution_channel_code,
        coalesce(`/BIC/GMATERIAL`   ,'Not Set')                as material_code,
        coalesce(`/BIC/GC_0003`     ,'Not Set')                as material_prodh5,
        coalesce(`/BIC/GC_0024`     ,'Not Set')                as material_prodh3,
        coalesce(`/BIC/GC_0025`     ,'Not Set')                as material_prodh2,
        coalesce(`/BIC/GC_0028`     ,'Not Set')                as material_prodh1,
        coalesce(`/BIC/GC_0036`     ,'Not Set')                as material_division,
        coalesce(`/BIC/GC_0064`     ,'Not Set')                as material_mkt_grp,
        coalesce(`/BIC/GC_0088`     ,'Not Set')                as material_prodh4,
        coalesce(`/BIC/GC_1860`     ,'Not Set')                as material_repprdc,
        coalesce(`/BIC/GC_1861`     ,'Not Set')                as material_repprdg1,
        coalesce(`/BIC/GC_1862`     ,'Not Set')                as material_repprdg2,
        coalesce(`/BIC/GC_1863`     ,'Not Set')                as material_repprdg3,
        coalesce(`/BIC/GC_1864`     ,'Not Set')                as material_repprdg4,
        coalesce(`/BIC/GPRODHIER`   ,'Not Set')                as material_prodhier,
        coalesce(`/BIC/GORGUNIT`    ,'Not Set')                as org_unit,
        coalesce(`/BIC/GSALESORG`   ,'Not Set')                as sales_org,
        coalesce(`/BIC/GVTYPE`      ,'Not Set')                as value_type_code,
        coalesce(`/BIC/UBILL_TYP`   ,'Not Set')                as billing_type_code,
        coalesce(`/BIC/UC_0501`     ,'Not Set')                as campaign_code,
        coalesce(`/BIC/UC_1183`     ,'Not Set')                as buying_group,
        coalesce(`/BIC/UDIVISION`   ,'Not Set')                as division_sales_code,
        coalesce(`/BIC/GC_1343`     ,'Not Set')                as customer_soldto_industry_nace_code,
        coalesce(`/BIC/GINDUSTRY`   ,'Not Set')                as customer_soldto_industry_miele_code,
        coalesce(`/BIC/UC_0502`     ,'Not Set')                as soldto_business_partner_type_code,
        coalesce(`/BIC/UVERSION`    ,'Not Set')                as version_code,
        coalesce(`/BIC/ZC_1647`     ,'Not Set')                as order_number,
        coalesce(`/BIC/ZC_1663`     ,'Not Set')                as order_line_item,
        coalesce(`/BIC/ZC_1648`     ,'Not Set')                as customer_soldto_code,
        coalesce(`/BIC/GC_1352`     ,'Not Set')                as org_consumer_classification_chain_code,
        coalesce(`UNIT`             ,'Not Set')                as uom,
        coalesce(`CURRENCY`         ,'Not Set')                as currency_code,
        cast(coalesce(`/BIC/ZK_1832`   ,0)  as decimal(19,2))  as amt_net_sales_inv_II_ext_act,
        cast(coalesce(`/BIC/ZK_1833`   ,0)  as decimal(19,2))  as amt_consumer_discount,
        cast(coalesce(`/BIC/ZK_1834`   ,0)  as decimal(19,0))  as quantity,
        cast(coalesce(`/BIC/ZK_1835`   ,0)  as decimal(19,0))  as quantity_inv_appl_div81,
        cast(coalesce(`/BIC/ZK_1836`   ,0)  as decimal(19,0))  as quantity_pl_actual_all,
        hash_pk,
        meta_src_folder,
        meta_insert_ts                          as meta_src_insert_ts
    from source 

),

--extract only the records with an amount
source_enriched_amount as (

    select *
    from source_enriched
    where (amt_net_sales_inv_II_ext_act is not null and amt_net_sales_inv_II_ext_act<>0) or (amt_consumer_discount is not null and amt_consumer_discount<>0)

),

--extract only the records with a quantity
source_enriched_quantity as (

    select *
    from source_enriched
    where (quantity is not null and quantity <> 0) or (quantity_inv_appl_div81 is not null and quantity_inv_appl_div81<>0) or (quantity_pl_actual_all is not null and quantity_pl_actual_all<>0)

),

--using a full join combine back the entire dataset from the beginning having amounts and quantity on a single row, instead of separate (as it was on the beginning)
source_enriched_fulljoin as (
    select 
        coalesce(a.date_posting_copa, b.date_posting_copa)                                              as date_posting_copa,
        coalesce(a.reporting_unit_code, b.reporting_unit_code)                                          as reporting_unit_code,
        coalesce(a.crm_transaction_type_code, b.crm_transaction_type_code)                              as crm_transaction_type_code,
        coalesce(a.distribution_channel_code, b.distribution_channel_code)                              as distribution_channel_code,
        coalesce(a.material_code, b.material_code)                                                      as material_code,
        coalesce(a.material_prodh5, b.material_prodh5)                                                  as material_prodh5,
        coalesce(a.material_prodh3, b.material_prodh3)                                                  as material_prodh3,
        coalesce(a.material_prodh2, b.material_prodh2)                                                  as material_prodh2,
        coalesce(a.material_prodh1, b.material_prodh1)                                                  as material_prodh1,
        coalesce(a.material_division, b.material_division)                                              as material_division,
        coalesce(a.material_mkt_grp, b.material_mkt_grp)                                                as material_mkt_grp,
        coalesce(a.material_prodh4, b.material_prodh4)                                                  as material_prodh4,
        coalesce(a.material_repprdc, b.material_repprdc)                                                as material_repprdc,
        coalesce(a.material_repprdg1, b.material_repprdg1)                                              as material_repprdg1,
        coalesce(a.material_repprdg2, b.material_repprdg2)                                              as material_repprdg2,
        coalesce(a.material_repprdg3, b.material_repprdg3)                                              as material_repprdg3,
        coalesce(a.material_repprdg4, b.material_repprdg4)                                              as material_repprdg4,
        coalesce(a.material_prodhier, b.material_prodhier)                                              as material_prodhier,
        coalesce(a.org_unit, b.org_unit)                                                                as org_unit,
        coalesce(a.sales_org, b.sales_org)                                                              as sales_org,
        coalesce(a.value_type_code, b.value_type_code)                                                  as value_type_code,
        coalesce(a.billing_type_code, b.billing_type_code)                                              as billing_type_code,
        coalesce(a.campaign_code, b.campaign_code)                                                      as campaign_code,
        coalesce(a.order_line_item, b.order_line_item)                                                  as order_line_item,
        coalesce(a.buying_group, b.buying_group)                                                        as buying_group,
        coalesce(a.division_sales_code, b.division_sales_code)                                          as division_sales_code,
        coalesce(a.customer_soldto_industry_nace_code, b.customer_soldto_industry_nace_code)            as customer_soldto_industry_nace_code,
        coalesce(a.customer_soldto_industry_miele_code, b.customer_soldto_industry_miele_code)          as customer_soldto_industry_miele_code,
        coalesce(a.soldto_business_partner_type_code, b.soldto_business_partner_type_code)              as soldto_business_partner_type_code,
        coalesce(a.version_code, b.version_code)                                                        as version_code,
        coalesce(a.order_number, b.order_number)                                                        as order_number,
        coalesce(a.customer_soldto_code, b.customer_soldto_code)                                        as customer_soldto_code,
        coalesce(a.org_consumer_classification_chain_code, b.org_consumer_classification_chain_code)    as org_consumer_classification_chain_code,
        coalesce(b.uom, a.uom)                                                                          as uom,
        coalesce(a.currency_code, b.currency_code)                                                      as currency_code,
        coalesce(a.amt_net_sales_inv_II_ext_act, b.amt_net_sales_inv_II_ext_act)                        as amt_net_sales_inv_II_ext_act,
        coalesce(a.amt_consumer_discount, b.amt_consumer_discount)                                      as amt_consumer_discount,
        coalesce(b.quantity, a.quantity)                                                                as quantity,
        coalesce(b.quantity_inv_appl_div81, a.quantity_inv_appl_div81)                                  as quantity_inv_appl_div81,
        coalesce(b.quantity_pl_actual_all, a.quantity_pl_actual_all)                                    as quantity_pl_actual_all,
        coalesce(a.hash_pk, b.hash_pk)                                                                  as hash_pk,
        coalesce(a.meta_src_folder, b.meta_src_folder)                                                  as meta_src_folder,
        coalesce(a.meta_src_insert_ts, b.meta_src_insert_ts)                                            as meta_src_insert_ts
    from source_enriched_amount a
    full join source_enriched_quantity b
        on a.hash_pk = b.hash_pk
),

final as (
    select distinct
        date_posting_copa                                                                       as date_posting_copa,
        reporting_unit_code                                                                     as reporting_unit_code,
        crm_transaction_type_code                                                               as crm_transaction_type_code,
        distribution_channel_code                                                               as distribution_channel_code,
        material_code                                                                           as material_code,
        material_prodh5                                                                         as material_prodh5,
        material_prodh3                                                                         as material_prodh3,
        material_prodh2                                                                         as material_prodh2,
        material_prodh1                                                                         as material_prodh1,
        material_division                                                                       as material_division,
        material_mkt_grp                                                                        as material_mkt_grp,
        material_prodh4                                                                         as material_prodh4,
        material_repprdc                                                                        as material_repprdc,
        material_repprdg1                                                                       as material_repprdg1,
        material_repprdg2                                                                       as material_repprdg2,
        material_repprdg3                                                                       as material_repprdg3,
        material_repprdg4                                                                       as material_repprdg4,
        material_prodhier                                                                       as material_prodhier,
        org_unit                                                                                as org_unit,
        sales_org                                                                               as sales_org,
        value_type_code                                                                         as value_type_code,
        billing_type_code                                                                       as billing_type_code,
        campaign_code                                                                           as campaign_code,
        order_line_item                                                                         as order_line_item,
        buying_group                                                                            as buying_group,
        division_sales_code                                                                     as division_sales_code,
        customer_soldto_industry_nace_code                                                      as customer_soldto_industry_nace_code,
        customer_soldto_industry_miele_code                                                     as customer_soldto_industry_miele_code,
        soldto_business_partner_type_code                                                       as soldto_business_partner_type_code,
        version_code                                                                            as version_code,
        order_number                                                                            as order_number,
        customer_soldto_code                                                                    as customer_soldto_code,
        org_consumer_classification_chain_code                                                  as org_consumer_classification_chain_code,
        uom                                                                                     as uom,
        currency_code                                                                           as currency_code,
        amt_net_sales_inv_II_ext_act                                                            as amt_net_sales_inv_II_ext_act,
        amt_consumer_discount                                                                   as amt_consumer_discount,
        quantity                                                                                as quantity,
        quantity_inv_appl_div81                                                                 as quantity_inv_appl_div81,
        quantity_pl_actual_all                                                                  as quantity_pl_actual_all,
        hash_pk                                                                                 as hash_pk,
        'NEXT3'                                                                                 as source_id,
        'UVGR_M01_Q0460'                                                                        as meta_extraction_query,
        'UCOPAD26(DSO)'                                                                         as meta_bw_export_structure,
        case when org_consumer_classification_chain_code = 'Z000000520' then 'Y' else 'N' end   as is_ecomm,
        case when soldto_business_partner_type_code='Z003' then 'Y' else 'N' end                as is_individual_consumer,
        'N'                                                                                     as is_amazon ,
        'N'                                                                                     as is_bu_professional,
        meta_src_folder                                                                         as meta_src_folder,
        meta_src_insert_ts                                                                      as meta_src_insert_ts,
        current_timestamp()                                                                     as meta_insert_ts
    from source_enriched_fulljoin
)

select *
from final