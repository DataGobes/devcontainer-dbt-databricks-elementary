{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      

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

vgd_sd_order_type as (
    select * from {{ ref('dim_sapbw_vgd_7_08_sd_order_type') }} 
),

vgd_doc_type as (
    select * from {{ ref('dim_sapbw_vgd_6_04_document_type') }} 
),

vgd_sales_grp as (
    select * from {{ ref('dim_sapbw_vgd_4_06_sales_group') }} 
),

vgd_doc_categ as (
    select * from {{ ref('dim_sapbw_vgd_6_08_document_category') }} 
),

n3_crm_tr_type as (
    select * from {{ ref('dim_sapbw_crm_transaction_type') }} 
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

n3_ind_nace as (
    select * from {{ ref('dim_sapbw_n3_industry_nace') }} 
),

n3_ind_miele as (
    select * from {{ ref('dim_sapbw_n3_industry_miele') }} 
),

vgd_ind_miele as (
    select * from {{ ref('dim_sapbw_vgd_3_31_sold_to_industry') }} 
),

n3_bp_type as (
    select * from {{ ref('dim_sapbw_soldto_business_partner_type') }} 
),

n3_miele_club as (
    select * from {{ ref('dim_sapbw_n3_cust_grp3_miele_club') }} 
),

n3_int_acc_cust as (
    select * from {{ ref('dim_sapbw_n3_int_account_customer') }} 
),

n3_campaign as (
    select * from {{ ref('dim_sapbw_campaign') }} 
),

vgd_campaign as (
    select * from {{ ref('dim_sapbw_vgd_6_09_campaign') }} 
),

n3_div_sales as (
    select * from {{ ref('dim_sapbw_division_sales') }} 
),

vgd_div_sales as (
    select * from {{ ref('dim_sapbw_vgd_7_99_division') }} 
),

n3_item_ctg as (
    select * from {{ ref('dim_sapbw_n3_item_category') }} 
),

vgd_item_ctg as (
    select * from {{ ref('dim_sapbw_vgd_6_23_item_category') }} 
),

n3_cons_class_chain as (
    select * from {{ ref('dim_sapbw_org_consumer_classification_chain') }} 
),

n3_distr_channel as (
    select * from {{ ref('dim_sapbw_distribution_channel') }} 
),

vgd_distr_channel as (
    select * from {{ ref('dim_sapbw_vgd_7_01_distribution_channel') }} 
),

vgd_order_reason as (
    select * from {{ ref('dim_sapbw_vgd_7_04_order_reason') }} 
),

n3_reason_rejection as (
    select * from {{ ref('dim_sapbw_n3_reason_rejection') }} 
),

vgd_reason_rejection as (
    select * from {{ ref('dim_sapbw_vgd_7_05_reason_rejection') }} 
),

n3_entry_channel as (
    select * from {{ ref('dim_sapbw_n3_entry_channel') }} 
),

n3_status_deliv as (
    select * from {{ ref('dim_sapbw_n3_status_delivered') }} 
),

n3_status_sys_hdr as (
    select * from {{ ref('dim_sapbw_n3_status_order_creation') }} 
),

vgd_status_deliv as (
    select * from {{ ref('dim_sapbw_vgd_6_07_status_delivered') }} 
),

vgd_status_rejec as (
    select * from {{ ref('dim_sapbw_vgd_9_03_status_rejection') }} 
),

n3_status_qual as (
    select * from {{ ref('dim_sapbw_n3_status_quality') }} 
),

vgd_val_view as (
    select * from {{ ref('dim_sapbw_vgd_9_99_valuation_view') }} 
),

vgd_value_type as (
    select * from {{ ref('dim_sapbw_vgd_9_20_value_type') }} 
),

vgd_version as (
    select * from {{ ref('dim_sapbw_vgd_6_30_version') }} 
),

vgd_prg_series as (
    select * from {{ ref('dim_sapbw_vgd_7_07_program_series') }} 
),

n3_dealer_agent as (
    select * from {{ ref('dim_sapbw_n3_dealer_agent') }} 
),

n3_buy_grp as (
    select * from {{ ref('dim_sapbw_buying_group') }} 
),

sap_bw_uom as (
    select * from {{ ref('dim_sapbw_uom') }} 
),

pim_product_master as (
    select * from {{ source('src_dim_product_master', 'dim_product_master_pim') }} 
),


vgd as (
    select
        base_vgd.date_doc_creation                                                  as date_doc_creation,
        cast(left(cast(base_vgd.date_doc_creation as varchar(10)),6) as bigint)     as yr_mth,
        base_vgd.date_requested_delivery                                            as date_requested_delivery,
        base_vgd.date_confirmed_delivery                                            as date_confirmed_delivery,
        base_vgd.date_actual_delivery                                               as date_actual_delivery,
        base_vgd.date_document_change                                               as date_document_change,
        base_vgd.date_extraction_DSP                                                as date_extraction_DSP,
        base_vgd.order_number                                                       as order_number,
        base_vgd.order_line_item                                                    as order_line_item,
        coalesce(vg_ref.vg,'Not Set')                                               as vg,
        base_vgd.reporting_unit_code                                                as reporting_unit_code,
        coalesce(rep_unit.text_medium, 'Not Set')                                   as reporting_unit_name,
        base_vgd.sales_org                                                          as sales_org,
        coalesce(sales_org.text_medium, 'Not Set')                                  as sales_org_name,
        'Not Set'                                                                   as org_unit,
        'Not Set'                                                                   as org_unit_name,
        base_vgd.customer_order_type                                                as customer_order_type,
        coalesce(vgd_sd_order_type.member_desc, 'Not Set')                          as customer_order_type_name,
        base_vgd.sales_doc_type                                                     as sales_doc_type,
        coalesce(vgd_doc_type.member_desc, 'Not Set')                               as sales_doc_type_name,
        base_vgd.sales_group_code                                                   as sales_group_code,
        coalesce(vgd_sales_grp.member_desc, 'Not Set')                              as sales_group_name,
        base_vgd.document_category_code                                             as document_category_code,
        coalesce(vgd_doc_categ.member_desc, 'Not Set')                              as document_category_name,
        'Not Set'                                                                   as crm_transaction_type_code,
        'Not Set'                                                                   as crm_transaction_type_name,
        base_vgd.material_code                                                      as material_code,
        coalesce(pim_product_master.dim_product_master_id,'-1')                     as dim_product_master_id,
        base_vgd.customer_account_grp_code                                          as customer_account_group_code,
        coalesce(vgd_cust_acc_grp.account_group_name, 'Not Set')                    as customer_account_group_name,
        base_vgd.customer_grp_code                                                  as customer_group_code, 
        coalesce(vgd_cust_grp.member_desc, 'Not Set')                               as customer_group_name,
        base_vgd.customer_postal_code                                               as customer_postal_code,
        base_vgd.customer_association_grp_code                                      as customer_association_group_code,
        coalesce(vgd_cust_assc_grp.member_desc, 'Not Set')                          as customer_association_group_name,
        'Not Set'                                                                   as customer_soldto_industry_nace_code,
        'Not Set'                                                                   as customer_soldto_industry_nace_name,
        base_vgd.customer_soldto_industry_miele_code                                as customer_soldto_industry_miele_code,
        coalesce(vgd_ind_miele.member_desc, 'Not Set')                              as customer_soldto_industry_miele_name,
        base_vgd.customer_soldto_code                                               as customer_soldto_code,
        'Not Set'                                                                   as soldto_business_partner_type_code,
        'Not Set'                                                                   as soldto_business_partner_type_name,
        'Not Set'                                                                   as soldto_cust_group3_miele_club,
        'Not Set'                                                                   as soldto_cust_group3_miele_club_name,
        'Not Set'                                                                   as soldto_internat_acc_customer,
        'Not Set'                                                                   as soldto_internat_acc_customer_name,
        base_vgd.campaign_code                                                      as campaign_code,
        coalesce(vgd_campaign.member_desc, 'Not Set')                               as campaign_name,
        base_vgd.division_sales_code                                                as division_sales_code,
        coalesce(vgd_div_sales.member_desc, 'Not Set')                              as division_sales_name,
        base_vgd.item_category_code                                                 as item_category_code,
        coalesce(vgd_item_ctg.member_desc, 'Not Set')                               as item_category_name,
        'Not Set'                                                                   as org_consumer_classification_chain_code,
        'Not Set'                                                                   as org_consumer_classification_chain_name,
        base_vgd.distribution_channel_code                                          as distribution_channel_code,
        coalesce(vgd_distr_channel.member_desc, 'Not Set')                          as distribution_channel_name,
        base_vgd.reason_order_code                                                  as reason_order_code,
        coalesce(vgd_order_reason.member_desc, 'Not Set')                           as reason_order_name,
        base_vgd.reason_rejection_code                                              as reason_rejection_code,
        coalesce(vgd_reason_rejection.member_desc, 'Not Set')                       as reason_rejection_name,
        'Not Set'                                                                   as entry_channel,
        'Not Set'                                                                   as entry_channel_name,
        base_vgd.status_delivered                                                   as status_delivered,
        coalesce(vgd_status_deliv.member_desc, 'Not Set')                           as status_delivered_name,
        base_vgd.status_rejection                                                   as status_rejection,
        coalesce(vgd_status_rejec.member_desc, 'Not Set')                           as status_rejection_name,
        'Not Set'                                                                   as status_system_header,
        'Not Set'                                                                   as status_system_header_name,
        'Not Set'                                                                   as status_quality,
        'Not Set'                                                                   as status_quality_name,
        'Not Set'                                                                   as project_id,
        'Not Set'                                                                   as header_item_indicator,
        base_vgd.valuation_view                                                     as valuation_view,
        coalesce(vgd_val_view.member_desc, 'Not Set')                               as valuation_view_name,
        base_vgd.value_type_code                                                    as value_type_code,
        coalesce(vgd_value_type.member_desc, 'Not Set')                             as value_type_name,
        base_vgd.version_code                                                       as version_code,
        coalesce(vgd_version.member_desc, 'Not Set')                                as version_name,
        base_vgd.program_series                                                     as program_series,
        coalesce(vgd_prg_series.member_desc, 'Not Set')                             as program_series_name,
        'Not Set'                                                                   as dealer_agent,
        'Not Set'                                                                   as dealer_agent_name,
        'Not Set'                                                                   as buying_group,
        'Not Set'                                                                   as buying_group_name,
        base_vgd.currency_local                                                     as currency_local,
        base_vgd.currency_document                                                  as currency_document,
        case when base_vgd.currency_code='Not Set' then base_vgd.currency_document else base_vgd.currency_code end as currency_code,
        base_vgd.currency_type                                                      as currency_type,
        base_vgd.uom                                                                as uom,
        coalesce(sap_bw_uom.member_desc,'Not Set')                                  as uom_name,
        base_vgd.sales_unit_of_measure                                              as sales_unit_of_measure,
        base_vgd.volume_unit                                                        as volume_unit,
        base_vgd.quantity                                                           as quantity,
        base_vgd.quantity_uom                                                       as quantity_uom,
        base_vgd.quantity_intake                                                    as quantity_intake,
        base_vgd.quantity_intake_uom                                                as quantity_intake_uom,
        base_vgd.amt_net_value                                                      as amt_net_value,
        base_vgd.currency_net_value                                                 as currency_net_value,
        base_vgd.amt_net_intake_value                                               as amt_net_intake_value,
        base_vgd.currency_net_intake_value                                          as currency_net_intake_value,
        base_vgd.amt_net_price                                                      as amt_net_price,
        base_vgd.currency_net_price                                                 as currency_net_price,
        is_ecomm                                                                    as is_ecomm,
        is_individual_consumer                                                      as is_individual_consumer,
        is_amazon                                                                   as is_amazon,
        is_bu_professional                                                          as is_bu_professional,
        base_vgd.source_id			     	    	                                as source_id,
        base_vgd.hash_pk						                                    as hash_pk,
        meta_extraction_query                                                       as meta_extraction_query,
        meta_bw_export_structure                                                    as meta_bw_export_structure,
        meta_src_folder                                                             as meta_src_folder
    from {{ ref('enr_sapbw_sales_order_details_vgd') }} base_vgd 
    left join rep_unit 
        on base_vgd.reporting_unit_code                  = rep_unit.reporting_unit_code
    left join  vg_ref
        on  base_vgd.reporting_unit_code                 = vg_ref.rep_unit_code
    left join sales_org 
        on base_vgd.sales_org                            = sales_org.sales_org
    left join vgd_sd_order_type 
        on base_vgd.customer_order_type                  = vgd_sd_order_type.member_code
    left join vgd_doc_type 
        on base_vgd.sales_doc_type                       = vgd_doc_type.member_code
    left join vgd_sales_grp 
        on base_vgd.sales_group_code                     = vgd_sales_grp.member_code
    left join vgd_doc_categ 
        on base_vgd.document_category_code               = vgd_doc_categ.member_code
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
    left join vgd_campaign  
        on base_vgd.campaign_code                        = vgd_campaign.member_code
    left join vgd_div_sales 
        on base_vgd.division_sales_code                  = vgd_div_sales.member_code 
    left join vgd_item_ctg 
        on base_vgd.item_category_code                   = vgd_item_ctg.member_code 
    left join vgd_distr_channel 
        on base_vgd.distribution_channel_code            = vgd_distr_channel.member_code 
    left join vgd_order_reason 
        on base_vgd.reason_order_code                    = vgd_order_reason.member_code 
    left join vgd_reason_rejection 
        on base_vgd.reason_rejection_code                = vgd_reason_rejection.member_code 
    left join vgd_status_deliv 
        on base_vgd.status_delivered                     = vgd_status_deliv.member_code 
    left join vgd_status_rejec 
        on base_vgd.status_rejection                     = vgd_status_rejec.member_code 
    left join vgd_val_view 
        on base_vgd.valuation_view                       = vgd_val_view.member_code 
    left join vgd_value_type 
        on base_vgd.value_type_code                      = vgd_value_type.member_code 
    left join vgd_version 
        on base_vgd.version_code                         = vgd_version.member_code 
    left join vgd_prg_series 
        on base_vgd.program_series                       = vgd_prg_series.member_code 
    left join sap_bw_uom
        on base_vgd.uom                                  = sap_bw_uom.member_code
),

next3 as (   
    select
        base_n3.date_doc_creation                                                   as date_doc_creation,
        cast(left(cast(base_n3.date_doc_creation as varchar(10)),6) as bigint)      as yr_mth,
        base_n3.date_requested_delivery                                             as date_requested_delivery,
        base_n3.date_confirmed_delivery                                             as date_confirmed_delivery,
        base_n3.date_actual_delivery                                                as date_actual_delivery,
        19700101                                                                    as date_document_change,
        19700101                                                                    as date_extraction_DSP,
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
        'Not Set'                                                                   as sales_doc_type,
        'Not Set'                                                                   as sales_doc_type_name,
        'Not Set'                                                                   as sales_group_code,
        'Not Set'                                                                   as sales_group_name,
        'Not Set'                                                                   as document_category_code,
        'Not Set'                                                                   as document_category_name,
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
        base_n3.soldto_cust_group3_miele_club                                       as soldto_cust_group3_miele_club,
        coalesce(n3_miele_club.member_desc,'Not Set')                               as soldto_cust_group3_miele_club_name,
        base_n3.soldto_internat_acc_customer                                        as soldto_internat_acc_customer,
        coalesce(n3_int_acc_cust.int_acct_customer_desc,'Not Set')                  as soldto_internat_acc_customer_name,
        base_n3.campaign_code                                                       as campaign_code,
        coalesce(n3_campaign.text_medium,'Not Set')                                 as campaign_name,
        base_n3.division_sales_code                                                 as division_sales_code,
        coalesce(n3_div_sales.text_medium,'Not Set')                                as division_sales_name,
        base_n3.item_category_code                                                  as item_category_code,
        coalesce(n3_item_ctg.member_desc,'Not Set')                                 as item_category_name,
        base_n3.org_consumer_classification_chain_code                              as org_consumer_classification_chain_code,
        coalesce(n3_cons_class_chain.text_medium,'Not Set')                         as org_consumer_classification_chain_name,
        base_n3.distribution_channel_code                                           as distribution_channel_code,
        coalesce(n3_distr_channel.text_short,'Not Set')                             as distribution_channel_name,
        'Not Set'                                                                   as reason_order_code,
        'Not Set'                                                                   as reason_order_name,
        base_n3.reason_rejection_code                                               as reason_rejection_code,
        coalesce(n3_reason_rejection.member_desc,'Not Set')                         as reason_rejection_name,
        base_n3.entry_channel                                                       as entry_channel,
        coalesce(n3_entry_channel.member_desc,'Not Set')                            as entry_channel_name,
        base_n3.status_delivered                                                    as status_delivered,
        coalesce(n3_status_deliv.member_desc,'Not Set')                             as status_delivered_name,
        'Not Set'                                                                   as status_rejection,
        'Not Set'                                                                   as status_rejection_name,
        base_n3.status_system_header                                                as status_system_header,
        coalesce(n3_status_sys_hdr.member_desc,'Not Set')                           as status_system_header_name,
        base_n3.status_quality                                                      as status_quality,
        coalesce(n3_status_qual.member_desc,'Not Set')                              as status_quality_name,
        project_id                                                                  as project_id,
        header_item_indicator                                                       as header_item_indicator,
        'Not Set'                                                                   as valuation_view,
        'Not Set'                                                                   as valuation_view_name,
        'Not Set'                                                                   as value_type_code,
        'Not Set'                                                                   as value_type_name,
        'Not Set'                                                                   as version_code,
        'Not Set'                                                                   as version_name,
        'Not Set'                                                                   as program_series,
        'Not Set'                                                                   as program_series_name,
        base_n3.dealer_agent                                                        as dealer_agent,
        coalesce(n3_dealer_agent.dealer_agent_desc,'Not Set')                       as dealer_agent_name,
        base_n3.buying_group                                                        as buying_group,
        coalesce(n3_buy_grp.text_medium,'Not Set')                                  as buying_group_name,
        'Not Set'                                                                   as currency_local,
        base_n3.currency_code                                                       as currency_document,
        base_n3.currency_code                                                       as currency_code,
        'Not Set'                                                                   as currency_type,
        base_n3.uom                                                                 as uom,
        coalesce(sap_bw_uom.member_desc,'Not Set')                                  as uom_name,
        'Not Set'                                                                   as sales_unit_of_measure,
        base_n3.volume_unit                                                         as volume_unit,
        base_n3.quantity                                                            as quantity,
        base_n3.uom                                                                 as quantity_uom,
        0                                                                           as quantity_intake,
        'Not Set'                                                                   as quantity_intake_uom,
        base_n3.amt_net_value                                                       as amt_net_value,
        base_n3.currency_net_value                                                  as currency_net_value,
        0                                                                           as amt_net_intake_value,
        'Not Set'                                                                   as currency_net_intake_value,
        0                                                                           as amt_net_price,
        'Not Set'                                                                   as currency_net_price,
        is_ecomm                                                                    as is_ecomm,
        is_individual_consumer                                                      as is_individual_consumer,
        is_amazon                                                                   as is_amazon,
        is_bu_professional                                                          as is_bu_professional,
        base_n3.source_id			     	    	                                as source_id,
        base_n3.hash_pk						                                        as hash_pk,
        base_n3.meta_extraction_query                                               as meta_extraction_query,
        base_n3.meta_bw_export_structure                                            as meta_bw_export_structure,
        base_n3.meta_src_folder                                                     as meta_src_folder
    from {{ ref('enr_sapbw_sales_order_details_next3') }} base_n3 
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
    left join n3_miele_club
        on base_n3.soldto_cust_group3_miele_club        = n3_miele_club.member_code
    left join n3_int_acc_cust
        on base_n3.reporting_unit_code                  = n3_int_acc_cust.rep_unit_code
        and base_n3.soldto_internat_acc_customer        = n3_int_acc_cust.int_acct_customer_code
    left join n3_campaign
        on base_n3.reporting_unit_code                  = n3_campaign.reporting_unit_code
        and base_n3.campaign_code                       = n3_campaign.campaign_code
    left join n3_div_sales
        on base_n3.reporting_unit_code                  = n3_div_sales.reporting_unit_code
        and base_n3.division_sales_code                 = n3_div_sales.division_sales_code 
    left join n3_item_ctg
        on base_n3.item_category_code                   = n3_item_ctg.member_code  
    left join n3_cons_class_chain
        on base_n3.org_consumer_classification_chain_code = n3_cons_class_chain.org_consumer_classification_chain_code 
    left join n3_distr_channel
        on base_n3.distribution_channel_code            = n3_distr_channel.distribution_channel_code  
    left join n3_reason_rejection
        on base_n3.reason_rejection_code                = n3_reason_rejection.member_code  
    left join n3_entry_channel
        on base_n3.entry_channel                        = n3_entry_channel.member_code   
    left join n3_status_deliv
        on base_n3.status_delivered                     = n3_status_deliv.member_code  
    left join n3_status_sys_hdr
        on base_n3.status_system_header                 = n3_status_sys_hdr.member_code   
    left join n3_status_qual
        on base_n3.status_quality                       = n3_status_qual.member_code     
    left join n3_dealer_agent 
        on base_n3.reporting_unit_code                  = n3_dealer_agent.rep_unit_code
        and base_n3.dealer_agent                        = n3_dealer_agent.dealer_agent_code
    left join n3_buy_grp 
        on base_n3.reporting_unit_code                  = n3_buy_grp.reporting_unit_code
        and base_n3.buying_group                        = n3_buy_grp.buying_group_code
    left join sap_bw_uom
        on base_n3.uom                                  = sap_bw_uom.member_code
 ),


all as (
    select
        date_doc_creation,
        yr_mth,
        date_requested_delivery,
        date_confirmed_delivery,
        date_actual_delivery,
        date_document_change,
        date_extraction_DSP,
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
        sales_doc_type,
        sales_doc_type_name,
        sales_group_code,
        sales_group_name,
        document_category_code,
        document_category_name,
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
        soldto_cust_group3_miele_club,
        soldto_cust_group3_miele_club_name,
        soldto_internat_acc_customer,
        soldto_internat_acc_customer_name,
        campaign_code,
        campaign_name,
        division_sales_code,
        division_sales_name,
        item_category_code,
        item_category_name,
        org_consumer_classification_chain_code,
        org_consumer_classification_chain_name,
        distribution_channel_code,
        distribution_channel_name,
        reason_order_code,
        reason_order_name,
        reason_rejection_code,
        reason_rejection_name,
        entry_channel,
        entry_channel_name,
        status_delivered,
        status_delivered_name,
        status_rejection,
        status_rejection_name,
        status_system_header,
        status_system_header_name,
        status_quality,
        status_quality_name,
        project_id,
        header_item_indicator,
        valuation_view,
        valuation_view_name,
        value_type_code,
        value_type_name,
        version_code,
        version_name,
        program_series,
        program_series_name,
        dealer_agent,
        dealer_agent_name,
        buying_group,
        buying_group_name,
        currency_local,
        currency_document,
        currency_code,
        currency_type,
        uom,
        uom_name,
        sales_unit_of_measure,
        volume_unit,
        quantity,
        quantity_uom,
        quantity_intake,
        quantity_intake_uom,
        amt_net_value,
        currency_net_value,
        amt_net_intake_value,
        currency_net_intake_value,
        amt_net_price,
        currency_net_price,
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
        date_doc_creation,
        yr_mth,
        date_requested_delivery,
        date_confirmed_delivery,
        date_actual_delivery,
        date_document_change,
        date_extraction_DSP,
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
        sales_doc_type,
        sales_doc_type_name,
        sales_group_code,
        sales_group_name,
        document_category_code,
        document_category_name,
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
        soldto_cust_group3_miele_club,
        soldto_cust_group3_miele_club_name,
        soldto_internat_acc_customer,
        soldto_internat_acc_customer_name,
        campaign_code,
        campaign_name,
        division_sales_code,
        division_sales_name,
        item_category_code,
        item_category_name,
        org_consumer_classification_chain_code,
        org_consumer_classification_chain_name,
        distribution_channel_code,
        distribution_channel_name,
        reason_order_code,
        reason_order_name,
        reason_rejection_code,
        reason_rejection_name,
        entry_channel,
        entry_channel_name,
        status_delivered,
        status_delivered_name,
        status_rejection,
        status_rejection_name,
        status_system_header,
        status_system_header_name,
        status_quality,
        status_quality_name,
        project_id,
        header_item_indicator,
        valuation_view,
        valuation_view_name,
        value_type_code,
        value_type_name,
        version_code,
        version_name,
        program_series,
        program_series_name,
        dealer_agent,
        dealer_agent_name,
        buying_group,
        buying_group_name,
        currency_local,
        currency_document,
        currency_code,
        currency_type,
        uom,
        uom_name,
        sales_unit_of_measure,
        volume_unit,
        quantity,
        quantity_uom,
        quantity_intake,
        quantity_intake_uom,
        amt_net_value,
        currency_net_value,
        amt_net_intake_value,
        currency_net_intake_value,
        amt_net_price,
        currency_net_price,
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
        all.date_doc_creation as date_key,
        all.date_doc_creation,
        all.yr_mth,
        all.date_requested_delivery,
        all.date_confirmed_delivery,
        all.date_actual_delivery,
        all.date_document_change,
        all.date_extraction_DSP,
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
        all.sales_doc_type,
        all.sales_doc_type_name,
        all.sales_group_code,
        all.sales_group_name,
        all.document_category_code,
        all.document_category_name,
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
        all.soldto_cust_group3_miele_club,
        all.soldto_cust_group3_miele_club_name,
        all.soldto_internat_acc_customer,
        all.soldto_internat_acc_customer_name,
        all.campaign_code,
        all.campaign_name,
        all.division_sales_code,
        all.division_sales_name,
        all.item_category_code,
        all.item_category_name,
        all.org_consumer_classification_chain_code,
        all.org_consumer_classification_chain_name,
        all.distribution_channel_code,
        all.distribution_channel_name,
        all.reason_order_code,
        all.reason_order_name,
        all.reason_rejection_code,
        all.reason_rejection_name,
        all.entry_channel,
        all.entry_channel_name,
        all.status_delivered,
        all.status_delivered_name,
        all.status_rejection,
        all.status_rejection_name,
        all.status_system_header,
        all.status_system_header_name,
        all.status_quality,
        all.status_quality_name,
        all.project_id,
        all.header_item_indicator,
        all.valuation_view,
        all.valuation_view_name,
        all.value_type_code,
        all.value_type_name,
        all.version_code,
        all.version_name,
        all.program_series,
        all.program_series_name,
        all.dealer_agent,
        all.dealer_agent_name,
        all.buying_group,
        all.buying_group_name,
        all.currency_local,
        all.currency_document,
        all.currency_code,
        all.currency_type,
        all.uom,
        all.uom_name,
        all.sales_unit_of_measure,
        all.volume_unit,
        all.quantity,
        all.quantity_uom,
        all.quantity_intake,
        all.quantity_intake_uom,
        all.amt_net_value,
        case when fx_rate.fx_rate is null or fx_rate.fx_rate=0 then 0 else case when all.currency_document='EUR' then all.amt_net_value else all.amt_net_value/fx_rate.fx_rate end end as amt_net_value_EUR,
        all.currency_net_value,
        all.amt_net_intake_value,
        case when fx_rate.fx_rate is null or fx_rate.fx_rate=0 then 0 else case when all.currency_document='EUR' then all.amt_net_intake_value else all.amt_net_intake_value/fx_rate.fx_rate end end as amt_net_intake_value_EUR,
        all.currency_net_intake_value,
        all.amt_net_price,
        case when fx_rate.fx_rate is null or fx_rate.fx_rate=0 then 0 else case when all.currency_document='EUR' then all.amt_net_price else all.amt_net_price/fx_rate.fx_rate end end as amt_net_price_EUR,
        all.currency_net_price,
        all.is_ecomm,
        all.is_individual_consumer ,
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
       and all.currency_document = fx_rate.from_curr
)

select  *, 
        current_timestamp() as meta_insert_ts 
from final