{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"   
) }}
      
-- take the source data only with the flag active Y (the ones with a negative active filter are history records that will only live in the raw table)
with source as (

    select *
    from
        {{ source('src_sap_bw_restricted', 'sap_bw_sales_order_financials_vgd') }}
    where 
        is_active= 'Y' 
),

source_enriched as (    
    select
       cast(coalesce(`CALDAY`       ,19700101) as bigint)       as date_posting_copa,
       cast(coalesce(`/BIC/DBILLING`,19700101) as bigint)       as date_billing,
       cast(coalesce(`/BIC/DGI`     ,19700101) as bigint)       as date_good_issue,
       coalesce(`/BIC/CDOCNUM_X`        ,'Not Set')             as order_number,
       coalesce(`SORD_ITEM`             ,'Not Set')             as order_line_item,
       coalesce(`/BIC/CREP_UNIT`        ,'Not Set')             as reporting_unit_code,
       coalesce(`/BIC/CORD_TYPE`        ,'Not Set')             as customer_order_type,
       coalesce(`SALES_GRP`             ,'Not Set')             as sales_group_code,
       coalesce(`/BIC/CMATERIAL`        ,'Not Set')             as material_code,
       coalesce(`ACCNT_GRP`             ,'Not Set')             as customer_account_grp_code,
       coalesce(`CUST_GROUP`            ,'Not Set')             as customer_grp_code,
       coalesce(`POSTAL_CD`             ,'Not Set')             as customer_postal_code,
       coalesce(`/BIC/CASSO_GRP`        ,'Not Set')             as customer_association_grp_code,
       coalesce(`INDUSTRY`              ,'Not Set')             as customer_soldto_industry_miele_code,
       coalesce(`/BIC/CSOLDTO_X`        ,'Not Set')             as customer_soldto_code,
       coalesce(`VTYPE`                 ,'Not Set')             as value_type_code,
       coalesce(`/BIC/CVGD_PRR`         ,'Not Set')             as program_series,
       coalesce(`DISTR_CHAN`            ,'Not Set')             as distribution_channel_code,
       coalesce(`BILL_TYPE`             ,'Not Set')             as billing_type_code,
       coalesce(`ORD_REASON`            ,'Not Set')             as reason_order_code,
       coalesce(`VALUATION`             ,'Not Set')             as valuation_view,
       coalesce(`VERSION`               ,'Not Set')             as version_code,
       coalesce(`UNIT`                  ,'Not Set')             as uom,
       cast(coalesce(`NET_SAL_INV_II`   ,0)  as decimal(19,2))  as amt_net_sales_inv_II_ext_act,
       coalesce(`NET_SAL_CURR`          ,'Not Set')             as currency_code,
       cast(coalesce(`CONSUMER_DISCOUNT`,0)  as decimal(19,2))  as amt_consumer_discount,
       coalesce(`CONS_DISCOUNT_CURR`    ,'Not Set')             as currency_code_discount,
       cast(coalesce(`QUANTITY`         ,0)  as decimal(19,0))  as quantity,
        hash_pk                                                 as hash_pk,
        meta_src_folder                                         as meta_src_folder,
        meta_insert_ts                                          as meta_src_insert_ts
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
    where quantity is not null and quantity <> 0

),

--using a full join combine back the entire dataset from the beginning having amounts and quantity on a single row, instead of separate (as it was on the beginning)
enriched_full_join as (
    select 
        coalesce(a.date_posting_copa, b.date_posting_copa)                                          as date_posting_copa,
        coalesce(a.date_billing, b.date_billing)                                                    as date_billing,
        coalesce(a.date_good_issue, b.date_good_issue)                                              as date_good_issue,
        coalesce(a.order_number, b.order_number)                                                    as order_number,
        coalesce(a.order_line_item, b.order_line_item)                                              as order_line_item,
        coalesce(a.reporting_unit_code, b.reporting_unit_code)                                      as reporting_unit_code,
        coalesce(a.customer_order_type, b.customer_order_type)                                      as customer_order_type,
        coalesce(a.sales_group_code, b.sales_group_code)                                            as sales_group_code,
        coalesce(a.material_code, b.material_code)                                                  as material_code,
        coalesce(a.customer_account_grp_code, b.customer_account_grp_code)                          as customer_account_grp_code,
        coalesce(a.customer_grp_code, b.customer_grp_code)                                          as customer_grp_code,
        coalesce(a.customer_postal_code, b.customer_postal_code)                                    as customer_postal_code,
        coalesce(a.customer_association_grp_code, b.customer_association_grp_code)                  as customer_association_grp_code,
        coalesce(a.customer_soldto_industry_miele_code, b.customer_soldto_industry_miele_code)      as customer_soldto_industry_miele_code,
        coalesce(a.customer_soldto_code, b.customer_soldto_code)                                    as customer_soldto_code,
        coalesce(a.value_type_code, b.value_type_code)                                              as value_type_code,
        coalesce(a.program_series, b.program_series)                                                as program_series,
        coalesce(a.distribution_channel_code, b.distribution_channel_code)                          as distribution_channel_code,
        coalesce(a.billing_type_code, b.billing_type_code)                                          as billing_type_code,
        coalesce(a.reason_order_code, b.reason_order_code)                                          as reason_order_code,
        coalesce(a.valuation_view, b.valuation_view)                                                as valuation_view,
        coalesce(a.version_code, b.version_code)                                                    as version_code,
        coalesce(b.uom,a.uom)                                                                       as uom,
        coalesce(a.currency_code, b.currency_code)                                                  as currency_code,
        coalesce(a.currency_code_discount, b.currency_code_discount)                                as currency_code_discount,
        coalesce(b.quantity,a.quantity)                                                             as quantity,
        coalesce(a.amt_net_sales_inv_II_ext_act, b.amt_net_sales_inv_II_ext_act)                    as amt_net_sales_inv_II_ext_act,
        coalesce(a.amt_consumer_discount, b.amt_consumer_discount)                                  as amt_consumer_discount,
        coalesce(a.hash_pk, b.hash_pk)                                                              as hash_pk,
        coalesce(a.meta_src_folder, b.meta_src_folder)                                              as meta_src_folder,
        coalesce(a.meta_src_insert_ts, b.meta_src_insert_ts)                                        as meta_src_insert_ts
    from source_enriched_amount a
    full join source_enriched_quantity b
        on a.hash_pk = b.hash_pk
),

final as (

    select 
        date_posting_copa                                                                           as date_posting_copa,
        date_billing                                                                                as date_billing,
        date_good_issue                                                                             as date_good_issue,
        order_number                                                                                as order_number,
        order_line_item                                                                             as order_line_item,
        reporting_unit_code                                                                         as reporting_unit_code,
        customer_order_type                                                                         as customer_order_type,
        sales_group_code                                                                            as sales_group_code,
        material_code                                                                               as material_code,
        customer_account_grp_code                                                                   as customer_account_grp_code,
        customer_grp_code                                                                           as customer_grp_code,
        customer_postal_code                                                                        as customer_postal_code,
        customer_association_grp_code                                                               as customer_association_grp_code,
        customer_soldto_industry_miele_code                                                         as customer_soldto_industry_miele_code,
        customer_soldto_code                                                                        as customer_soldto_code,
        value_type_code                                                                             as value_type_code,
        program_series                                                                              as program_series,
        distribution_channel_code                                                                   as distribution_channel_code,
        billing_type_code                                                                           as billing_type_code,
        reason_order_code                                                                           as reason_order_code,
        valuation_view                                                                              as valuation_view,
        version_code                                                                                as version_code,
        uom                                                                                         as uom,
        currency_code                                                                               as currency_code,
        currency_code_discount                                                                      as currency_code_discount,
        quantity                                                                                    as quantity,
        amt_net_sales_inv_II_ext_act                                                                as amt_net_sales_inv_II_ext_act,
        amt_consumer_discount                                                                       as amt_consumer_discount,
        'VGD'                                                                                       as source_id,
        'HVGD100_Q017'                                                                              as meta_extraction_query,
        'AVGD800(aDSO)'                                                                             as meta_bw_export_structure,
        case when sales_group_code='B2C' 
            and (customer_soldto_industry_miele_code <> '0301' and customer_soldto_industry_miele_code <>'0308') 
            and (customer_order_type = '#' or customer_order_type = 'ABO' or customer_order_type = 'B2C') 
            then 'Y' else 'N' end                                                                   as is_ecomm,
        case when (customer_soldto_industry_miele_code ='0303') 
            and (customer_account_grp_code='B201' or customer_account_grp_code='B202') 
            then 'Y' else 'N' end                                                                   as is_individual_consumer, 
        'N'                                                                                         as is_amazon ,
        'N'                                                                                         as is_bu_professional,
        hash_pk                                                                                     as hash_pk,
        meta_src_folder                                                                             as meta_src_folder,
        meta_src_insert_ts                                                                          as meta_src_insert_ts,
        current_timestamp()                                                                         as meta_insert_ts
    from enriched_full_join
)



select *
from final 