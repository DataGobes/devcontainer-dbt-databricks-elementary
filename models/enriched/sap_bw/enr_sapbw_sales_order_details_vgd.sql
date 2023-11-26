{{ config(
    schema = "restricted_enriched",
    materialized = "table"  
) }}
      
-- take the source data only with the flag active Y (the ones with a negative active filter are history records that will only live in the raw table)
with source as (

    select *
    from
        {{ source('src_sap_bw_restricted', 'sap_bw_sales_order_details_vgd') }}
    where 
        is_active= 'Y' 
),

source_enriched as(
    select 
        cast(coalesce(`CREATEDON`       ,19700101) as bigint)           as date_doc_creation,
        cast(coalesce(`/BIC/DREQDLV`    ,19700101) as bigint)           as date_requested_delivery,
        cast(coalesce(`CONF_DATE`       ,19700101) as bigint)           as date_confirmed_delivery,
        cast(coalesce(`ACT_DL_DTE`      ,19700101) as bigint)           as date_actual_delivery,
        cast(coalesce(`AEDAT`           ,19700101) as bigint)           as date_document_change,
        cast(coalesce(`/BIC/DEXTRACT`   ,19700101) as bigint)           as date_extraction_DSP,
        coalesce(`/BIC/CDOCNUM_X`       ,'Not Set')                     as order_number,
        coalesce(`SORD_ITEM`            ,'Not Set')                     as order_line_item,
        coalesce(`/BIC/CREP_UNIT`       ,'Not Set')                     as reporting_unit_code,
        coalesce(`SALESORG`             ,'Not Set')                     as sales_org,
        coalesce(`/BIC/CORD_TYPE`       ,'Not Set')                     as customer_order_type,
        coalesce(`DOC_TYPE`             ,'Not Set')                     as sales_doc_type,
        coalesce(`SALES_GRP`            ,'Not Set')                     as sales_group_code,
        coalesce(`DOC_CAT`              ,'Not Set')                     as document_category_code,
        coalesce(`/BIC/CMATERIAL`       ,'Not Set')                     as material_code,
        coalesce(`ACCNT_GRP`            ,'Not Set')                     as customer_account_grp_code,
        coalesce(`CUST_GROUP`           ,'Not Set')                     as customer_grp_code,
        coalesce(`POSTAL_CD`            ,'Not Set')                     as customer_postal_code,
        coalesce(`/BIC/CASSO_GRP`       ,'Not Set')                     as customer_association_grp_code,
        coalesce(`INDUSTRY`             ,'Not Set')                     as customer_soldto_industry_miele_code,
        coalesce(`/BIC/CSOLDTO_X`       ,'Not Set')                     as customer_soldto_code,
        coalesce(`LOC_CURRCY`           ,'Not Set')                     as currency_local,
        coalesce(`DOC_CURRCY`           ,'Not Set')                     as currency_document,
        coalesce(`CURRENCY`             ,'Not Set')                     as currency_code,
        coalesce(`CURTYPE`              ,'Not Set')                     as currency_type,
        coalesce(`/BIC/CCPGN_VGD`       ,'Not Set')                     as campaign_code,
        coalesce(`DIVISION`             ,'Not Set')                     as division_sales_code,
        coalesce(`ITEM_CATEG`           ,'Not Set')                     as item_category_code,
        coalesce(`DISTR_CHAN`           ,'Not Set')                     as distribution_channel_code,
        coalesce(`ORD_REASON`           ,'Not Set')                     as reason_order_code,
        coalesce(`REASON_REJ`           ,'Not Set')                     as reason_rejection_code,
        coalesce(`DLV_STS`              ,'Not Set')                     as status_delivered,
        coalesce(`REJECTN_ST`           ,'Not Set')                     as status_rejection,
        coalesce(`VALUATION`            ,'Not Set')                     as valuation_view,
        coalesce(`VTYPE`                ,'Not Set')                     as value_type_code,
        coalesce(`VERSION`              ,'Not Set')                     as version_code,
        coalesce(`/BIC/CVGD_PRR`        ,'Not Set')                     as program_series,
        coalesce(`UNIT`                 ,'Not Set')                     as uom,
        coalesce(`SALES_UNIT`           ,'Not Set')                     as sales_unit_of_measure,
        coalesce(`VOLUMEUNIT`           ,'Not Set')                     as volume_unit,
        cast(coalesce(`ORD_QTY`,0)         as decimal(19,0))            as quantity,
        coalesce(`ORD_QTY_UNIT`         ,'Not Set')                     as quantity_uom,
        cast(coalesce(`ORD_INT_QTY`,0)     as decimal(19,0))            as quantity_intake,
        coalesce(`ORD_INT_QTY_UNIT`     ,'Not Set')                     as quantity_intake_uom,
        cast(coalesce(`NET_VALUE`,0)       as decimal(19,2))            as amt_net_value,
        coalesce(`NET_VALUE_CURR`       ,'Not Set')                     as currency_net_value,
        cast(coalesce(`NET_ORD_INT_VAL`,0) as decimal(19,2))            as amt_net_intake_value,
        coalesce(`NET_ORD_INT_VAL_CURR` ,'Not Set')                     as currency_net_intake_value,
        cast(coalesce(`NET_PRICE`,0)       as decimal(19,2))            as amt_net_price,
        coalesce(`NET_PRICE_CURR`       ,'Not Set')                     as currency_net_price,
        hash_pk,
        meta_src_folder,
        meta_insert_ts as meta_src_insert_ts
    from source
),

--extract only the records with an amount
source_enriched_amount as (

    select *
    from source_enriched
    where amt_net_intake_value is not null or amt_net_price is not null or amt_net_value is not null 
    and (amt_net_intake_value <> 0 or amt_net_price <> 0 or amt_net_value  <> 0)

),

--extract only the records with a quantity
source_enriched_quantity as (

    select *
    from source_enriched
     where quantity is not null and quantity_intake is not null and ( quantity <> 0 or quantity_intake <> 0)

),

--using a full join combine back the entire dataset from the beginning having amounts and quantity on a single row, instead of separate (as it was on the beginning)
source_enriched_fulljoin as (
    select 
        coalesce(a.date_doc_creation, b.date_doc_creation)                                          as date_doc_creation,
        coalesce(a.date_requested_delivery, b.date_requested_delivery)                              as date_requested_delivery,
        coalesce(a.date_confirmed_delivery, b.date_confirmed_delivery)                              as date_confirmed_delivery,
        coalesce(a.date_actual_delivery, b.date_actual_delivery)                                    as date_actual_delivery,
        coalesce(a.date_document_change, b.date_document_change)                                    as date_document_change,
        coalesce(a.date_extraction_DSP, b.date_extraction_DSP)                                      as date_extraction_DSP,
        coalesce(a.order_number, b.order_number)                                                    as order_number,
        coalesce(a.order_line_item, b.order_line_item)                                              as order_line_item,
        coalesce(a.reporting_unit_code, b.reporting_unit_code)                                      as reporting_unit_code,
        coalesce(a.sales_org, b.sales_org)                                                          as sales_org,
        coalesce(a.customer_order_type, b.customer_order_type)                                      as customer_order_type,
        coalesce(a.sales_doc_type, b.sales_doc_type)                                                as sales_doc_type,
        coalesce(a.sales_group_code, b.sales_group_code)                                            as sales_group_code,
        coalesce(a.document_category_code, b.document_category_code)                                as document_category_code,
        coalesce(a.material_code, b.material_code)                                                  as material_code,
        coalesce(a.customer_account_grp_code, b.customer_account_grp_code)                          as customer_account_grp_code,
        coalesce(a.customer_grp_code, b.customer_grp_code)                                          as customer_grp_code,
        coalesce(a.customer_postal_code, b.customer_postal_code)                                    as customer_postal_code,
        coalesce(a.customer_association_grp_code, b.customer_association_grp_code)                  as customer_association_grp_code,
        coalesce(a.customer_soldto_industry_miele_code, b.customer_soldto_industry_miele_code)      as customer_soldto_industry_miele_code,
        coalesce(a.customer_soldto_code, b.customer_soldto_code)                                    as customer_soldto_code,
        coalesce(a.currency_local, b.currency_local)                                                as currency_local,
        coalesce(a.currency_document, b.currency_document)                                          as currency_document,
        coalesce(a.currency_code, b.currency_code)                                                  as currency_code,
        coalesce(a.currency_type, b.currency_type)                                                  as currency_type,
        coalesce(a.campaign_code, b.campaign_code)                                                  as campaign_code,
        coalesce(a.division_sales_code, b.division_sales_code)                                      as division_sales_code,
        coalesce(a.item_category_code, b.item_category_code)                                        as item_category_code,
        coalesce(a.distribution_channel_code, b.distribution_channel_code)                          as distribution_channel_code,
        coalesce(a.reason_order_code, b.reason_order_code)                                          as reason_order_code,
        coalesce(a.reason_rejection_code, b.reason_rejection_code)                                  as reason_rejection_code,
        coalesce(a.status_delivered, b.status_delivered)                                            as status_delivered,
        coalesce(a.status_rejection, b.status_rejection)                                            as status_rejection,
        coalesce(a.valuation_view, b.valuation_view)                                                as valuation_view,
        coalesce(a.value_type_code, b.value_type_code)                                              as value_type_code,
        coalesce(a.version_code, b.version_code)                                                    as version_code,
        coalesce(a.program_series, b.program_series)                                                as program_series,
        coalesce(a.uom, b.uom)                                                                      as uom,
        coalesce(a.sales_unit_of_measure, b.sales_unit_of_measure)                                  as sales_unit_of_measure,
        coalesce(a.volume_unit, b.volume_unit)                                                      as volume_unit,
        coalesce(b.quantity, a.quantity)                                                            as quantity,
        coalesce(b.quantity_uom, a.quantity_uom)                                                    as quantity_uom,
        coalesce(b.quantity_intake, a.quantity_intake)                                              as quantity_intake,
        coalesce(b.quantity_intake_uom, a.quantity_intake_uom)                                      as quantity_intake_uom,
        coalesce(a.amt_net_value, b.amt_net_value)                                                  as amt_net_value,
        coalesce(a.currency_net_value, b.currency_net_value)                                        as currency_net_value,
        coalesce(a.amt_net_intake_value, b.amt_net_intake_value)                                    as amt_net_intake_value,
        coalesce(a.currency_net_intake_value, b.currency_net_intake_value)                          as currency_net_intake_value,
        coalesce(a.amt_net_price, b.amt_net_price)                                                  as amt_net_price,
        coalesce(a.currency_net_price, b.currency_net_price)                                        as currency_net_price,
        coalesce(a.hash_pk, b.hash_pk)                                                              as hash_pk,
        coalesce(a.meta_src_folder, b.meta_src_folder)                                              as meta_src_folder,
        coalesce(a.meta_src_insert_ts, b.meta_src_insert_ts)                                        as meta_src_insert_ts
    from source_enriched_amount a
    full join source_enriched_quantity b
        on a.hash_pk = b.hash_pk
),

final as (
    select distinct
        date_doc_creation                                                                   as date_doc_creation,
        date_requested_delivery                                                             as date_requested_delivery,
        date_confirmed_delivery                                                             as date_confirmed_delivery,
        date_actual_delivery                                                                as date_actual_delivery,
        date_document_change                                                                as date_document_change,
        date_extraction_DSP                                                                 as date_extraction_DSP,
        order_number                                                                        as order_number,
        order_line_item                                                                     as order_line_item,
        reporting_unit_code                                                                 as reporting_unit_code,
        sales_org                                                                           as sales_org,
        customer_order_type                                                                 as customer_order_type,
        sales_doc_type                                                                      as sales_doc_type,
        sales_group_code                                                                    as sales_group_code,
        document_category_code                                                              as document_category_code,
        material_code                                                                       as material_code,
        customer_account_grp_code                                                           as customer_account_grp_code,
        customer_grp_code                                                                   as customer_grp_code,
        customer_postal_code                                                                as customer_postal_code,
        customer_association_grp_code                                                       as customer_association_grp_code,
        customer_soldto_industry_miele_code                                                 as customer_soldto_industry_miele_code,
        customer_soldto_code                                                                as customer_soldto_code,
        currency_local                                                                      as currency_local,
        currency_document                                                                   as currency_document,
        currency_code                                                                       as currency_code,
        currency_type                                                                       as currency_type,
        campaign_code                                                                       as campaign_code,
        division_sales_code                                                                 as division_sales_code,
        item_category_code                                                                  as item_category_code,
        distribution_channel_code                                                           as distribution_channel_code,
        reason_order_code                                                                   as reason_order_code,
        reason_rejection_code                                                               as reason_rejection_code,
        status_delivered                                                                    as status_delivered,
        status_rejection                                                                    as status_rejection,
        valuation_view                                                                      as valuation_view,
        value_type_code                                                                     as value_type_code,
        version_code                                                                        as version_code,
        program_series                                                                      as program_series,
        uom                                                                                 as uom,
        sales_unit_of_measure                                                               as sales_unit_of_measure,
        volume_unit                                                                         as volume_unit,
        quantity                                                                            as quantity,
        quantity_uom                                                                        as quantity_uom,
        quantity_intake                                                                     as quantity_intake,
        quantity_intake_uom                                                                 as quantity_intake_uom,
        amt_net_value                                                                       as amt_net_value,
        currency_net_value                                                                  as currency_net_value,
        amt_net_intake_value                                                                as amt_net_intake_value,
        currency_net_intake_value                                                           as currency_net_intake_value,
        amt_net_price                                                                       as amt_net_price,
        currency_net_price                                                                  as currency_net_price,
        hash_pk                                                                             as hash_pk,
        'VGD'                                                                               as source_id,
        'HVGD100_Q032'                                                                      as meta_extraction_query,
        'AVGD801(aDSO)'                                                                     as meta_bw_export_structure,
        case when sales_group_code='B2C' 
            and (customer_soldto_industry_miele_code <> '0301' and customer_soldto_industry_miele_code <>'0308') 
            and (customer_order_type = '#' or customer_order_type = 'ABO' or customer_order_type = 'B2C') 
            then 'Y' else 'N' end                                                           as is_ecomm,
        case when (customer_soldto_industry_miele_code ='0303') 
            and (customer_account_grp_code='B201' or customer_account_grp_code='B202') 
            then 'Y' else 'N' end                                                           as is_individual_consumer, 
        'N'                                                                                 as is_amazon ,
        'N'                                                                                 as is_bu_professional,
        meta_src_folder                                                                     as meta_src_folder,
        meta_src_insert_ts                                                                  as meta_src_insert_ts,
        current_timestamp()                                                                 as meta_insert_ts

    from source_enriched_fulljoin

)

select *
from final 