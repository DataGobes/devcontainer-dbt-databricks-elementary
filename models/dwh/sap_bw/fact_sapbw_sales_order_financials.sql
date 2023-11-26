{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}

{{
  config(
    partition_by = "source_id"
  )
}}

with vg_ref as (
    select
        *
    from {{ ref('dim_sapbw_rep_unit_vg') }} 
    where vg is not null
),

rep_unit as (
    select * from {{ ref('dim_sapbw_reporting_unit') }} 
),

sales_org as (
    select * from {{ ref('dim_sapbw_sales_org') }} 
),

n3_org_unit as (
    select * from {{ ref('dim_sapbw_org_unit') }} where org_unit_order=1
),


n3_crm_tr_type as (
    select * from {{ ref('dim_sapbw_crm_transaction_type') }} 
),


vgd_sd_order_type as (
    select * from {{ ref('dim_sapbw_vgd_7_08_sd_order_type') }} 
),

vgd_sales_grp as (
    select * from {{ ref('dim_sapbw_vgd_4_06_sales_group') }} 
),

vgd_cust_acc_grp as (
    select * from {{ ref('dim_sapbw_vgd_3_53_cust_account_group') }} 
),

vgd_cust_grp as (
    select * from {{ ref('dim_sapbw_vgd_3_55_customer_group') }} 
),

vgd_cust_assc_grp as (
    select * from {{ ref('dim_sapbw_vgd_3_20_sold_to_cust_assoc_group') }} 
),

vgd_ind_miele as (
    select * from {{ ref('dim_sapbw_vgd_3_31_sold_to_industry') }} 
),

n3_ind_nace as (
    select * from {{ ref('dim_sapbw_n3_industry_nace') }} 
),

n3_ind_miele as (
    select * from {{ ref('dim_sapbw_n3_industry_miele') }} 
),

n3_bp_type as (
    select * from {{ ref('dim_sapbw_soldto_business_partner_type') }} 
),

vgd_value_type as (
    select * from {{ ref('dim_sapbw_vgd_9_20_value_type') }} 
),

vgd_prg_series as (
    select * from {{ ref('dim_sapbw_vgd_7_07_program_series') }} 
),

vgd_distr_channel as (
    select * from {{ ref('dim_sapbw_vgd_7_01_distribution_channel') }} 
),

vgd_6_22_billing_type as (
    select * from {{ ref('dim_sapbw_vgd_6_22_billing_type') }} 
),

vgd_order_reason as (
    select * from {{ ref('dim_sapbw_vgd_7_04_order_reason') }} 
),

vgd_val_view as (
    select * from {{ ref('dim_sapbw_vgd_9_99_valuation_view') }} 
),

vgd_version as (
    select * from {{ ref('dim_sapbw_vgd_6_30_version') }} 
),

n3_value_type as (
    select * from {{ ref('dim_sapbw_n3_value_type') }} 
),

n3_campaign as (
    select * from {{ ref('dim_sapbw_campaign') }} 
),

n3_div_sales as (
    select * from {{ ref('dim_sapbw_division_sales') }} 
),

n3_buy_grp as (
    select * from {{ ref('dim_sapbw_buying_group') }} 
),

n3_cons_class_chain as (
    select * from {{ ref('dim_sapbw_org_consumer_classification_chain') }} 
),

n3_distr_channel as (
    select * from {{ ref('dim_sapbw_distribution_channel') }} 
),

n3_billing_type as (
    select * from {{ ref('dim_sapbw_billing_type') }} 
),

n3_entry_channel as (
    select * from {{ ref('dim_sapbw_n3_entry_channel') }} 
),

entry_channel_derived as (
    select distinct a.reporting_unit_code, a.order_number, a.order_line_item, a.entry_channel, b.member_desc as entry_channel_name,
       is_bu_professional 
    from {{ ref('enr_sapbw_sales_order_details_next3') }} a 
    left join n3_entry_channel b 
        on a.entry_channel = b.member_code
),

sap_bw_uom as (
    select * from {{ ref('dim_sapbw_uom') }} 
),

pim_product_master as (
    select * from {{ source('src_dim_product_master', 'dim_product_master_pim') }} 
),


vgd as (
    select
        base_vgd.date_posting_copa                                                  as date_key,
        base_vgd.date_posting_copa                                                  as date_posting_copa,
        cast(left(cast(base_vgd.date_posting_copa as varchar(10)),6) as bigint)     as yr_mth,
        base_vgd.date_billing                                                       as date_billing,
        base_vgd.date_good_issue                                                    as date_good_issue,
        base_vgd.order_number                                                       as order_number,
        base_vgd.order_line_item                                                    as order_line_item,
        coalesce(vg_ref.vg,'Not Set')                                               as vg,
        base_vgd.reporting_unit_code                                                as reporting_unit_code,
        coalesce(rep_unit.text_medium,'Not Set')                                    as reporting_unit_name,
        'Not Set'                                                                   as sales_org,
        'Not Set'                                                                   as sales_org_name,
        'Not Set'                                                                   as org_unit,
        'Not Set'                                                                   as org_unit_name,
        base_vgd.customer_order_type                                                as customer_order_type,
        coalesce(vgd_sd_order_type.member_desc,'Not Set')                           as customer_order_type_name,
        base_vgd.sales_group_code                                                   as sales_group_code,
        coalesce(vgd_sales_grp.member_desc,'Not Set')                               as sales_group_name,
        'Not Set'                                                                   as crm_transaction_type_code,
        'Not Set'                                                                   as crm_transaction_type_name,
        base_vgd.material_code                                                      as material_code,
        coalesce(pim_product_master.dim_product_master_id,'-1')                     as dim_product_master_id,
        base_vgd.customer_account_grp_code                                          as customer_account_group_code,
        coalesce(vgd_cust_acc_grp.account_group_name,'Not Set')                     as customer_account_group_name,
        base_vgd.customer_grp_code                                                  as customer_group_code, 
        coalesce(vgd_cust_grp.member_desc,'Not Set')                                as customer_group_name,
        base_vgd.customer_postal_code                                               as customer_postal_code,
        base_vgd.customer_association_grp_code                                      as customer_association_group_code,
        coalesce(vgd_cust_assc_grp.member_desc,'Not Set')                           as customer_association_group_name,
        'Not Set'                                                                   as customer_soldto_industry_nace_code,
        'Not Set'                                                                   as customer_soldto_industry_nace_name,
        base_vgd.customer_soldto_industry_miele_code                                as customer_soldto_industry_miele_code,
        coalesce(vgd_ind_miele.member_desc,'Not Set')                               as customer_soldto_industry_miele_name,
        base_vgd.customer_soldto_code                                               as customer_soldto_code,
        'Not Set'                                                                   as soldto_business_partner_type_code,
        'Not Set'                                                                   as soldto_business_partner_type_name,
        base_vgd.value_type_code                                                    as value_type_code,
        coalesce(vgd_value_type.member_desc,'Not Set')                              as value_type_name,
        base_vgd.program_series                                                     as program_series,
        coalesce(vgd_prg_series.member_desc,'Not Set')                              as program_series_name,
        'Not Set'                                                                   as campaign_code,
        'Not Set'                                                                   as campaign_name,
        'Not Set'                                                                   as division_sales_code,
        'Not Set'                                                                   as division_sales_name,
        'Not Set'                                                                   as buying_group,
        'Not Set'                                                                   as buying_group_name,
        'Not Set'                                                                   as org_consumer_classification_chain_code,
        'Not Set'                                                                   as org_consumer_classification_chain_name,
        base_vgd.distribution_channel_code                                          as distribution_channel_code,
        coalesce(vgd_distr_channel.member_desc,'Not Set')                           as distribution_channel_name,
        base_vgd.billing_type_code                                                  as billing_type_code,
        coalesce(vgd_6_22_billing_type.member_desc,'Not Set')                       as billing_type_name,
        base_vgd.reason_order_code                                                  as reason_order_code,
        coalesce(vgd_order_reason.member_desc,'Not Set')                            as reason_order_name,
        base_vgd.valuation_view                                                     as valuation_view,
        coalesce(vgd_val_view.member_desc,'Not Set')                                as valuation_view_name,
        base_vgd.version_code                                                       as version_code,
        coalesce(vgd_version.member_desc,'Not Set')                                 as version_name,
        'Not Set'                                                                   as entry_channel,
        'Not Set'                                                                   as entry_channel_name,
        base_vgd.uom                                                                as uom,
        coalesce(sap_bw_uom.member_desc,'Not Set')                                  as uom_name,
        base_vgd.currency_code                                                      as currency_code,
        base_vgd.currency_code_discount                                             as currency_code_discount,
        base_vgd.amt_net_sales_inv_II_ext_act                                       as amt_net_sales_inv_II_ext_act,
        base_vgd.amt_consumer_discount                                              as amt_consumer_discount,
        base_vgd.quantity                                                           as quantity,
        0                                                                           as quantity_inv_appl_div81,
        0                                                                           as quantity_pl_actual_all,
        is_ecomm                                                                    as is_ecomm,
        is_individual_consumer                                                      as is_individual_consumer,
        is_amazon                                                                   as is_amazon,
        is_bu_professional                                                          as is_bu_professional,
        base_vgd.source_id			     	    	                                as source_id,
        base_vgd.hash_pk						                                    as hash_pk,
        meta_extraction_query                                                       as meta_extraction_query,
        meta_bw_export_structure                                                    as meta_bw_export_structure,
        meta_src_folder                                                             as meta_src_folder
    from {{ ref('enr_sapbw_sales_order_financials_vgd') }} base_vgd 
    left join rep_unit 
        on base_vgd.reporting_unit_code                  = rep_unit.reporting_unit_code
    left join  vg_ref
        on  base_vgd.reporting_unit_code                 = vg_ref.rep_unit_code
    left join vgd_sd_order_type 
        on base_vgd.customer_order_type                  = vgd_sd_order_type.member_code
    left join vgd_sales_grp 
        on base_vgd.sales_group_code                     = vgd_sales_grp.member_code
    left join pim_product_master
        on base_vgd.material_code                        = pim_product_master.prod_master_id
    left join vgd_cust_acc_grp 
        on base_vgd.customer_account_grp_code            = vgd_cust_acc_grp.customer_account_group_code
    left join vgd_cust_grp
        on base_vgd.customer_grp_code                    = vgd_cust_grp.member_code
    left join vgd_cust_assc_grp
        on base_vgd.customer_association_grp_code        = vgd_cust_assc_grp.member_code
    left join vgd_ind_miele
        on base_vgd.customer_soldto_industry_miele_code  = vgd_ind_miele.member_code 
    left join vgd_value_type 
        on base_vgd.value_type_code                      = vgd_value_type.member_code
    left join vgd_prg_series 
        on base_vgd.program_series                       = vgd_prg_series.member_code 
    left join vgd_distr_channel 
        on base_vgd.distribution_channel_code            = vgd_distr_channel.member_code 
    left join vgd_6_22_billing_type 
        on base_vgd.billing_type_code                    = vgd_6_22_billing_type.member_code 
    left join vgd_order_reason 
        on base_vgd.reason_order_code                    = vgd_order_reason.member_code 
    left join vgd_val_view 
        on base_vgd.valuation_view                       = vgd_val_view.member_code 
    left join vgd_version 
        on base_vgd.version_code                         = vgd_version.member_code
    left join sap_bw_uom
        on base_vgd.uom                                  = sap_bw_uom.member_code  
),

-- for the "entry channel" and "is_bu_professional" flag we retrieved this info not from the enriched of the SOF, but from enriched of the SOD
-- since this is only present in that dataset
next3 as (
    select
        base_n3.date_posting_copa                                                   as date_key,
        base_n3.date_posting_copa                                                   as date_posting_copa,
        cast(left(cast(base_n3.date_posting_copa as varchar(10)),6) as bigint)      as yr_mth,
        19700101                                                                    as date_billing,
        19700101                                                                    as date_good_issue,
        base_n3.order_number                                                        as order_number,
        base_n3.order_line_item                                                     as order_line_item,
        coalesce(vg_ref.vg,'Not Set')                                               as vg,
        base_n3.reporting_unit_code                                                 as reporting_unit_code,
        coalesce(rep_unit.text_medium,'Not Set')                                    as reporting_unit_name,
        base_n3.sales_org                                                           as sales_org,
        coalesce(sales_org.text_medium,'Not Set')                                   as sales_org_name,
        base_n3.org_unit                                                            as org_unit,
        coalesce(n3_org_unit.text_medium,'Not Set')                                 as org_unit_name,
        'Not Set'                                                                   as customer_order_type,
        'Not Set'                                                                   as customer_order_type_name,
        'Not Set'                                                                   as sales_group_code,
        'Not Set'                                                                   as sales_group_name,
        base_n3.crm_transaction_type_code                                           as crm_transaction_type_code,
        coalesce(n3_crm_tr_type.text_medium,'Not Set')                              as crm_transaction_type_name,
        base_n3.material_code                                                       as material_code,
        coalesce(pim_product_master.dim_product_master_id,'-1')                     as dim_product_master_id,        
        'Not Set'                                                                   as customer_account_group_code,
        'Not Set'                                                                   as customer_account_group_name,
        'Not Set'                                                                   as customer_group_code, 
        'Not Set'                                                                   as customer_group_name,
        'Not Set'                                                                   as customer_postal_code,
        'Not Set'                                                                   as customer_association_group_code,
        'Not Set'                                                                   as customer_association_group_name,
        base_n3.customer_soldto_industry_nace_code                                  as customer_soldto_industry_nace_code,
        coalesce(n3_ind_nace.member_desc,'Not Set')                                 as customer_soldto_industry_nace_name,
        base_n3.customer_soldto_industry_miele_code                                 as customer_soldto_industry_miele_code,
        coalesce(n3_ind_miele.member_desc,'Not Set')                                as customer_soldto_industry_miele_name,
        base_n3.customer_soldto_code                                                as customer_soldto_code,
        base_n3.soldto_business_partner_type_code                                   as soldto_business_partner_type_code,
        coalesce(n3_bp_type.text_medium,'Not Set')                                  as soldto_business_partner_type_name,
        base_n3.value_type_code                                                     as value_type_code,
        coalesce(n3_value_type.member_code,'Not Set')                               as value_type_name,
        'Not Set'                                                                   as program_series,
        'Not Set'                                                                   as program_series_name,
        base_n3.campaign_code                                                       as campaign_code,
        coalesce(n3_campaign.text_medium,'Not Set')                                 as campaign_name,
        base_n3.division_sales_code                                                 as division_sales_code,
        coalesce(n3_div_sales.text_medium,'Not Set')                                as division_sales_name,
        base_n3.buying_group                                                        as buying_group,
        coalesce(n3_buy_grp.text_medium,'Not Set')                                  as buying_group_name,
        base_n3.org_consumer_classification_chain_code                              as org_consumer_classification_chain_code,
        coalesce(n3_cons_class_chain.text_medium,'Not Set')                         as org_consumer_classification_chain_name,
        base_n3.distribution_channel_code                                           as distribution_channel_code,
        coalesce(n3_distr_channel.text_short,'Not Set')                             as distribution_channel_name,
        base_n3.billing_type_code                                                   as billing_type_code,
        coalesce(n3_billing_type.text_medium,'Not Set')                             as billing_type_name,
        'Not Set'                                                                   as reason_order_code,
        'Not Set'                                                                   as reason_order_name,
        'Not Set'                                                                   as valuation_view,
        'Not Set'                                                                   as valuation_view_name,
        base_n3.version_code                                                        as version_code,
        base_n3.version_code                                                        as version_name,
        coalesce(entry_channel_derived.entry_channel,'Not Set')                     as entry_channel,
        coalesce(entry_channel_derived.entry_channel_name,'Not Set')                as entry_channel_name,        
        base_n3.uom                                                                 as uom,
        coalesce(sap_bw_uom.member_desc,'Not Set')                                  as uom_name,
        base_n3.currency_code                                                       as currency_code,
        'Not Set'                                                                   as currency_code_discount,
        base_n3.amt_net_sales_inv_II_ext_act                                        as amt_net_sales_inv_II_ext_act,
        base_n3.amt_consumer_discount                                               as amt_consumer_discount,
        base_n3.quantity                                                            as quantity,
        base_n3.quantity_inv_appl_div81                                             as quantity_inv_appl_div81,
        base_n3.quantity_pl_actual_all                                              as quantity_pl_actual_all,
        is_ecomm                                                                    as is_ecomm,
        is_individual_consumer                                                      as is_individual_consumer,
        is_amazon                                                                   as is_amazon,
        coalesce(entry_channel_derived.is_bu_professional,'N')                      as is_bu_professional,
        base_n3.source_id			     	    	                                as source_id,
        base_n3.hash_pk				     		                                    as hash_pk,
        meta_extraction_query                                                       as meta_extraction_query,
        meta_bw_export_structure                                                    as meta_bw_export_structure,
        meta_src_folder                                                             as meta_src_folder
    from {{ ref('enr_sapbw_sales_order_financials_next3') }} base_n3 
    left join rep_unit 
        on base_n3.reporting_unit_code                  = rep_unit.reporting_unit_code
    left join  vg_ref
        on  base_n3.reporting_unit_code                 = vg_ref.rep_unit_code
    left join sales_org 
        on base_n3.sales_org                            = sales_org.sales_org
    left join n3_org_unit
        on base_n3.org_unit                             = n3_org_unit.org_unit
    left join n3_crm_tr_type
        on base_n3.crm_transaction_type_code            = n3_crm_tr_type.crm_transaction_type_code 
    left join pim_product_master
        on base_n3.material_code                        = pim_product_master.prod_master_id   
    left join n3_ind_nace
        on base_n3.customer_soldto_industry_nace_code   = n3_ind_nace.member_code  
    left join n3_ind_miele
        on base_n3.customer_soldto_industry_miele_code  = n3_ind_miele.member_code  
    left join n3_bp_type
        on base_n3.reporting_unit_code                  = n3_bp_type.reporting_unit_code
        and base_n3.soldto_business_partner_type_code   = n3_bp_type.soldto_business_partner_type_code
    left join n3_value_type
        on  base_n3.value_type_code                     = n3_value_type.member_code
    left join n3_campaign
        on base_n3.reporting_unit_code                  = n3_campaign.reporting_unit_code
        and base_n3.campaign_code                       = n3_campaign.campaign_code
    left join n3_div_sales
        on base_n3.reporting_unit_code                  = n3_div_sales.reporting_unit_code
        and base_n3.division_sales_code                 = n3_div_sales.division_sales_code 
    left join n3_buy_grp 
        on base_n3.reporting_unit_code                  = n3_buy_grp.reporting_unit_code
        and base_n3.buying_group                        = n3_buy_grp.buying_group_code
    left join n3_cons_class_chain
        on base_n3.org_consumer_classification_chain_code = n3_cons_class_chain.org_consumer_classification_chain_code 
    left join n3_distr_channel
        on base_n3.distribution_channel_code            = n3_distr_channel.distribution_channel_code  
    left join n3_billing_type
        on  base_n3.reporting_unit_code                 = n3_billing_type.reporting_unit_code
        and base_n3.billing_type_code                   = n3_billing_type.billing_type_code
    left join entry_channel_derived
        on  base_n3.reporting_unit_code                 = entry_channel_derived.reporting_unit_code
        and base_n3.order_number                        = entry_channel_derived.order_number
        and base_n3.order_line_item                     = entry_channel_derived.order_line_item
    left join sap_bw_uom
        on base_n3.uom                                  = sap_bw_uom.member_code
),

all as (
    select
        date_key,
        date_posting_copa,
        yr_mth,
        date_billing,
        date_good_issue,
        order_number,
        order_line_item,
        vg,
        reporting_unit_code,
        reporting_unit_name,
        sales_org,
        sales_org_name,
        org_unit,
        org_unit_name,
        customer_order_type,
        customer_order_type_name,
        sales_group_code,
        sales_group_name,
        crm_transaction_type_code,
        crm_transaction_type_name,
        material_code,
        dim_product_master_id,
        customer_account_group_code,
        customer_account_group_name,
        customer_group_code, 
        customer_group_name,
        customer_postal_code,
        customer_association_group_code,
        customer_association_group_name,
        customer_soldto_industry_nace_code,
        customer_soldto_industry_nace_name,
        customer_soldto_industry_miele_code,
        customer_soldto_industry_miele_name,
        customer_soldto_code,
        soldto_business_partner_type_code,
        soldto_business_partner_type_name,
        value_type_code,
        value_type_name,
        program_series,
        program_series_name,
        campaign_code,
        campaign_name,
        division_sales_code,
        division_sales_name,
        buying_group,
        buying_group_name,
        org_consumer_classification_chain_code,
        org_consumer_classification_chain_name,
        distribution_channel_code,
        distribution_channel_name,
        billing_type_code,
        billing_type_name,
        reason_order_code,
        reason_order_name,
        valuation_view,
        valuation_view_name,
        version_code,
        version_name,
        entry_channel,
        entry_channel_name,
        uom,
        uom_name,
        currency_code,
        currency_code_discount,
        amt_net_sales_inv_II_ext_act,
        amt_consumer_discount,
        quantity,
        quantity_inv_appl_div81,
        quantity_pl_actual_all,
        is_ecomm,
        is_individual_consumer,
        is_amazon,
        is_bu_professional,
        source_id,
        hash_pk,
        meta_extraction_query,
        meta_bw_export_structure,
        meta_src_folder  
    from vgd
    union
    select 
        date_key,
        date_posting_copa,
        yr_mth,
        date_billing,
        date_good_issue,
        order_number,
        order_line_item,
        vg,
        reporting_unit_code,
        reporting_unit_name,
        sales_org,
        sales_org_name,
        org_unit,
        org_unit_name,
        customer_order_type,
        customer_order_type_name,
        sales_group_code,
        sales_group_name,
        crm_transaction_type_code,
        crm_transaction_type_name,
        material_code,
        dim_product_master_id,
        customer_account_group_code,
        customer_account_group_name,
        customer_group_code, 
        customer_group_name,
        customer_postal_code,
        customer_association_group_code,
        customer_association_group_name,
        customer_soldto_industry_nace_code,
        customer_soldto_industry_nace_name,
        customer_soldto_industry_miele_code,
        customer_soldto_industry_miele_name,
        customer_soldto_code,
        soldto_business_partner_type_code,
        soldto_business_partner_type_name,
        value_type_code,
        value_type_name,
        program_series,
        program_series_name,
        campaign_code,
        campaign_name,
        division_sales_code,
        division_sales_name,
        buying_group,
        buying_group_name,
        org_consumer_classification_chain_code,
        org_consumer_classification_chain_name,
        distribution_channel_code,
        distribution_channel_name,
        billing_type_code,
        billing_type_name,
        reason_order_code,
        reason_order_name,
        valuation_view,
        valuation_view_name,
        version_code,
        version_name,
        entry_channel,
        entry_channel_name,
        uom,        
        uom_name,
        currency_code,
        currency_code_discount,
        amt_net_sales_inv_II_ext_act,
        amt_consumer_discount,
        quantity,
        quantity_inv_appl_div81,
        quantity_pl_actual_all,
        is_ecomm,
        is_individual_consumer,
        is_amazon,
        is_bu_professional,
        source_id,
        hash_pk,
        meta_extraction_query,
        meta_bw_export_structure,
        meta_src_folder    
    from next3
),

fx_rate as (
    select *
    from {{ source('src_reference', 'dim_fx_rate') }}
),


final as (
    select 
        all.date_key,
        all.date_posting_copa,
        all.yr_mth,
        all.date_billing,
        all.date_good_issue,
        all.order_number,
        all.order_line_item,
        all.vg,
        all.reporting_unit_code,
        all.reporting_unit_name,
        all.sales_org,
        all.sales_org_name,
        all.org_unit,
        all.org_unit_name,
        all.customer_order_type,
        all.customer_order_type_name,
        all.sales_group_code,
        all.sales_group_name,
        all.crm_transaction_type_code,
        all.crm_transaction_type_name,
        all.material_code,
        all.dim_product_master_id,
        all.customer_account_group_code,
        all.customer_account_group_name,
        all.customer_group_code, 
        all.customer_group_name,
        all.customer_postal_code,
        all.customer_association_group_code,
        all.customer_association_group_name,
        all.customer_soldto_industry_nace_code,
        all.customer_soldto_industry_nace_name,
        all.customer_soldto_industry_miele_code,
        all.customer_soldto_industry_miele_name,
        all.customer_soldto_code,
        all.soldto_business_partner_type_code,
        all.soldto_business_partner_type_name,
        all.value_type_code,
        all.value_type_name,
        all.program_series,
        all.program_series_name,
        all.campaign_code,
        all.campaign_name,
        all.division_sales_code,
        all.division_sales_name,
        all.buying_group,
        all.buying_group_name,
        all.org_consumer_classification_chain_code,
        all.org_consumer_classification_chain_name,
        all.distribution_channel_code,
        all.distribution_channel_name,
        all.billing_type_code,
        all.billing_type_name,
        all.reason_order_code,
        all.reason_order_name,
        all.valuation_view,
        all.valuation_view_name,
        all.version_code,
        all.version_name,
        all.entry_channel,
        all.entry_channel_name,
        all.uom,        
        all.uom_name,
        all.currency_code,
        all.currency_code_discount,
        all.amt_net_sales_inv_II_ext_act,
        case when fx_rate.fx_rate is null or fx_rate.fx_rate=0 then 0 else case when all.currency_code='EUR' then all.amt_net_sales_inv_II_ext_act else all.amt_net_sales_inv_II_ext_act/fx_rate.fx_rate end end as amt_net_sales_inv_II_ext_act_EUR,
        all.amt_consumer_discount,
        case when fx_rate.fx_rate is null or fx_rate.fx_rate=0 then 0 else case when all.currency_code='EUR' then all.amt_consumer_discount else all.amt_consumer_discount/fx_rate.fx_rate end end as amt_consumer_discount_EUR,
        all.quantity,
        all.quantity_inv_appl_div81,
        all.quantity_pl_actual_all,
        all.is_ecomm,
        all.is_individual_consumer,
        all.is_amazon,
        all.is_bu_professional,
        all.source_id,
        all.hash_pk,
        all.meta_extraction_query,
        all.meta_bw_export_structure,
        all.meta_src_folder    
    from all 
    left join fx_rate 
       on all.yr_mth = fx_rate.month_year
       and all.currency_code = fx_rate.from_curr
)

select *,
        current_timestamp() as meta_insert_ts 
 from final