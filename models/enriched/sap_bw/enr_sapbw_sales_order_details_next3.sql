{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"   
) }}
      

-- take the source data only with the flag active Y (the ones with a negative active filter are history records that will only live in the raw table)
-- also, as additional filter, some records need to be excluded (Saved Shopping Basket) -> where the status system header IS NOT 'I034' (Saved Shopping Basket)
-- these records are not real sales orders (yet) and may never become

with source as (
    select *
    from
        {{ source('src_sap_bw_restricted', 'sap_bw_sales_order_details_next3') }}
    where 
        is_active= 'Y' and (`/BIC/GOSTAT_H` is null or `/BIC/GOSTAT_H` <> 'I1034')
),

org_unit_ecomm as (
    select distinct 
        `/BIC/GC_0001`  as reporting_unit_code, 
        `/BIC/GORGUNIT` as org_unit 
    from {{ source('src_sap_bw_restricted', 'sap_bw_sales_order_financials_next3') }}
    where `/BIC/GC_1352`='Z000000520'
),

sod_n3_filter_crm_transaction_type as (

    select * from {{ ref('enr_sod_n3_filter_crm_transaction_type') }} 

),

source_enriched as (
    select 
        cast(coalesce(`CALDAY`         ,19700101) as bigint)        as date_doc_creation,
        cast(coalesce(`/BIC/ZDSDELDAT` ,19700101) as bigint)        as date_requested_delivery,
        cast(coalesce(`/BIC/ZT_1068`   ,19700101) as bigint)        as date_confirmed_delivery,
        cast(coalesce(`/BIC/ZT_0084`   ,19700101) as bigint)        as date_actual_delivery,
        coalesce(`/BIC/ZC_1647`  ,'Not Set')                        as order_number,
        coalesce(`/BIC/ZC_0008`  ,'Not Set')                        as order_line_item,
        coalesce(`/BIC/GC_0001`  ,'Not Set')                        as reporting_unit_code,
        coalesce(`/BIC/GSALESORG`,'Not Set')                        as sales_org,
        coalesce(`/BIC/GORGUNIT` ,'Not Set')                        as org_unit,
        coalesce(`/BIC/GC_PRCTYP`,'Not Set')                        as crm_transaction_type_code,
        coalesce(`/BIC/GMATERIAL`,'Not Set')                        as material_code,
        coalesce(`/BIC/GC_1343`  ,'Not Set')                        as customer_soldto_industry_nace_code,
        coalesce(`/BIC/GINDUSTRY`,'Not Set')                        as customer_soldto_industry_miele_code,
        coalesce(`/BIC/ZC_1648`  ,'Not Set')                        as customer_soldto_code,
        coalesce(`/BIC/UC_0502`  ,'Not Set')                        as soldto_business_partner_type_code,
        coalesce(`/BIC/UCUSTGRP3`,'Not Set')                        as soldto_cust_group3_miele_club,
        coalesce(`/BIC/GCUSTOMER`,'Not Set')                        as soldto_internat_acc_customer,
        coalesce(`/BIC/UC_0516`  ,'Not Set')                        as dealer_agent,
        coalesce(`/BIC/UC_1183`  ,'Not Set')                        as buying_group,
        coalesce(`/BIC/GCURRENCY`,'Not Set')                        as currency_code,
        coalesce(`/BIC/UC_0501`  ,'Not Set')                        as campaign_code,
        coalesce(`/BIC/UDIVISION`,'Not Set')                        as division_sales_code,
        coalesce(`/BIC/UITEM_CAT`,'Not Set')                        as item_category_code,
        coalesce(`/BIC/GC_1352`  ,'Not Set')                        as org_consumer_classification_chain_code,
        coalesce(`/BIC/GDISTR_CH`,'Not Set')                        as distribution_channel_code,
        coalesce(`/BIC/UCRMREJEC`,'Not Set')                        as reason_rejection_code,
        coalesce(`/BIC/GC_0023`  ,'Not Set')                        as entry_channel,
        coalesce(`/BIC/GC_0072`  ,'Not Set')                        as status_delivered,
        coalesce(`/BIC/GOSTAT_H` ,'Not Set')                        as status_system_header,
        coalesce(`/BIC/GC_0075`  ,'Not Set')                        as status_quality,
        coalesce(`/BIC/UC_1316`  ,'Not Set')                        as project_id,
        coalesce(`/BIC/ZC_0048`  ,'Not Set')                        as header_item_indicator,
        coalesce(`UNIT`          ,'Not Set')                        as uom,
        coalesce(`UNIT`          ,'Not Set')                        as volume_unit,
        cast(coalesce(`/BIC/ZQUANTITY`,0) as decimal(19,0))         as quantity,
        cast(coalesce(`/BIC/ZNETVAL2` ,0) as decimal(19,2))         as amt_net_value,
        coalesce(`/BIC/GCURRENCY`,'Not Set')                        as currency_net_value,
        hash_pk,
        meta_src_folder,
        meta_insert_ts      as meta_src_insert_ts
    from source
),


-- the raw data contains also consists opportunities and quotations, so these need to be filtered out
-- the sod_n3_filter_crm_transaction_type dataset contains only the values that correspond to sales orders
filter_raw_data as (
    select a.* from source_enriched a
    inner join sod_n3_filter_crm_transaction_type b 
        on a.crm_transaction_type_code = b.crm_transaction_type
),

--extract only the records with an amount
source_enriched_amount as (
    select *
    from filter_raw_data
    where amt_net_value is not null and  amt_net_value<>0
),

--extract only the records with a quantity
source_enriched_quantity as (
    select *
    from filter_raw_data
    where quantity is not null and quantity <> 0
),

--using a full join combine back the entire dataset from the beginning having amounts and quantity on a single row, instead of separate (as it was on the beginning)
source_enriched_fulljoin as (
    select
        coalesce(a.date_doc_creation,b.date_doc_creation)                                           as date_doc_creation,
        coalesce(a.date_requested_delivery,b.date_requested_delivery)                               as date_requested_delivery,
        coalesce(a.date_confirmed_delivery,b.date_confirmed_delivery)                               as date_confirmed_delivery,
        coalesce(a.date_actual_delivery,b.date_actual_delivery)                                     as date_actual_delivery,
        coalesce(a.order_number,b.order_number)                                                     as order_number,
        coalesce(a.order_line_item,b.order_line_item)                                               as order_line_item,
        coalesce(a.reporting_unit_code,b.reporting_unit_code)                                       as reporting_unit_code,
        coalesce(a.sales_org,b.sales_org)                                                           as sales_org,
        coalesce(a.org_unit,b.org_unit)                                                             as org_unit,
        coalesce(a.crm_transaction_type_code,b.crm_transaction_type_code)                           as crm_transaction_type_code,
        coalesce(a.material_code,b.material_code)                                                   as material_code,
        coalesce(a.customer_soldto_industry_nace_code,b.customer_soldto_industry_nace_code)         as customer_soldto_industry_nace_code,
        coalesce(a.customer_soldto_industry_miele_code,b.customer_soldto_industry_miele_code)       as customer_soldto_industry_miele_code,
        coalesce(a.customer_soldto_code,b.customer_soldto_code)                                     as customer_soldto_code,
        coalesce(a.soldto_business_partner_type_code,b.soldto_business_partner_type_code)           as soldto_business_partner_type_code,
        coalesce(a.soldto_cust_group3_miele_club,b.soldto_cust_group3_miele_club)                   as soldto_cust_group3_miele_club,
        coalesce(a.soldto_internat_acc_customer,b.soldto_internat_acc_customer)                     as soldto_internat_acc_customer,
        coalesce(a.dealer_agent,b.dealer_agent)                                                     as dealer_agent,
        coalesce(a.buying_group,b.buying_group)                                                     as buying_group,
        coalesce(a.currency_code,b.currency_code)                                                   as currency_code,
        coalesce(a.campaign_code,b.campaign_code)                                                   as campaign_code,
        coalesce(a.division_sales_code,b.division_sales_code)                                       as division_sales_code,
        coalesce(a.item_category_code,b.item_category_code)                                         as item_category_code,
        coalesce(a.org_consumer_classification_chain_code,b.org_consumer_classification_chain_code) as org_consumer_classification_chain_code,
        coalesce(a.distribution_channel_code,b.distribution_channel_code)                           as distribution_channel_code,
        coalesce(a.reason_rejection_code,b.reason_rejection_code)                                   as reason_rejection_code,
        coalesce(a.entry_channel,b.entry_channel)                                                   as entry_channel,
        coalesce(a.status_delivered,b.status_delivered)                                             as status_delivered,
        coalesce(a.status_system_header,b.status_system_header)                                     as status_system_header,
        coalesce(a.status_quality,b.status_quality)                                                 as status_quality,
        coalesce(a.project_id,b.project_id)                                                         as project_id,
        coalesce(a.header_item_indicator,b.header_item_indicator)                                   as header_item_indicator,
        coalesce(b.uom,a.uom)                                                                       as uom,
        coalesce(b.volume_unit,a.volume_unit)                                                       as volume_unit,
        coalesce(b.quantity,a.quantity)                                                             as quantity,
        coalesce(a.amt_net_value,b.amt_net_value)                                                   as amt_net_value,
        coalesce(a.currency_net_value,b.currency_net_value)                                         as currency_net_value,
        coalesce(a.hash_pk, b.hash_pk)                                                              as hash_pk,
        coalesce(a.meta_src_folder, b.meta_src_folder)                                              as meta_src_folder,
        coalesce(a.meta_src_insert_ts, b.meta_src_insert_ts)                                        as meta_src_insert_ts
    from source_enriched_amount a
    full join source_enriched_quantity b
    on a.hash_pk = b.hash_pk

),

final as (
    select distinct
        a.date_doc_creation                                                           as date_doc_creation,
        a.date_requested_delivery                                                     as date_requested_delivery,
        a.date_confirmed_delivery                                                     as date_confirmed_delivery,
        a.date_actual_delivery                                                        as date_actual_delivery,
        a.order_number                                                                as order_number,
        a.order_line_item                                                             as order_line_item,
        a.reporting_unit_code                                                         as reporting_unit_code,
        a.sales_org                                                                   as sales_org,
        a.org_unit                                                                    as org_unit,
        a.crm_transaction_type_code                                                   as crm_transaction_type_code,
        a.material_code                                                               as material_code,
        a.customer_soldto_industry_nace_code                                          as customer_soldto_industry_nace_code,
        a.customer_soldto_industry_miele_code                                         as customer_soldto_industry_miele_code,
        a.customer_soldto_code                                                        as customer_soldto_code,
        a.soldto_business_partner_type_code                                           as soldto_business_partner_type_code,
        a.soldto_cust_group3_miele_club                                               as soldto_cust_group3_miele_club,
        a.soldto_internat_acc_customer                                                as soldto_internat_acc_customer,
        a.dealer_agent                                                                as dealer_agent,
        a.buying_group                                                                as buying_group,
        a.currency_code                                                               as currency_code,
        a.campaign_code                                                               as campaign_code,
        a.division_sales_code                                                         as division_sales_code,
        a.item_category_code                                                          as item_category_code,
        a.org_consumer_classification_chain_code                                      as org_consumer_classification_chain_code,
        a.distribution_channel_code                                                   as distribution_channel_code,
        a.reason_rejection_code                                                       as reason_rejection_code,
        a.entry_channel                                                               as entry_channel,
        a.status_delivered                                                            as status_delivered,
        a.status_system_header                                                        as status_system_header,
        a.status_quality                                                              as status_quality,
        a.project_id                                                                  as project_id,
        a.header_item_indicator                                                       as header_item_indicator,
        a.uom                                                                         as uom,
        a.volume_unit                                                                 as volume_unit,
        a.quantity                                                                    as quantity,
        a.amt_net_value                                                               as amt_net_value,
        a.currency_net_value                                                          as currency_net_value,
        a.hash_pk                                                                     as hash_pk,
        'NEXT3'                                                                     as source_id,
        'UVGR_M01_Q0461'                                                            as meta_extraction_query,
        'UCOPAD27'                                                                  as meta_bw_export_structure,
        case when b.org_unit is null  then 'N' else 'Y' end                         as is_ecomm,
        case when a.soldto_business_partner_type_code='Z003' then 'Y' else 'N' end  as is_individual_consumer,
        'N'                                                                         as is_amazon ,
        case when a.entry_channel ='Z37' then 'Y' else 'N' end                      as is_bu_professional,
        a.meta_src_folder                                                           as meta_src_folder,
        a.meta_src_insert_ts                                                        as meta_src_insert_ts,
        current_timestamp()                                                         as meta_insert_ts
    from source_enriched_fulljoin a 
    left join org_unit_ecomm b 
        on  a.reporting_unit_code = b.reporting_unit_code
        and a.org_unit = b.org_unit
)


select * from final