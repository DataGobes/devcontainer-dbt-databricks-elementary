with source as (

    select *
    from
        {{ source('src_sap_c4c', 'sap_c4c_report_lead_funnel') }}
),

date_trim as (

    select
        *,
        replace(replace(CDOC_CHANGED_DT, '/DATE(', ''), ')/', '') / 1000 as changed_dt_epoch
    from source
),

renamed as (

    select
        CCAL_YEAR_M_LEAN_LEAD_START as month_year_predecessor_sales,
        TCAL_YEAR_M_LEAN_LEAD_START as month_year_text,
        CCCO_UUID                   as sales_order,
        TCCO_UUID                   as sales_order_text,
        CDOC_ID                     as lead_id,
        TDOC_ID                     as lead,
        CDOC_PRDC_ID                as campaign_id,
        TDOC_PRDC_ID                as campaign,
        CDPY_MAINPROSPCT            as account_id,
        TDPY_MAINPROSPCT            as account,
        CDPY_PRSPY_CNY              as country_region_id,
        TDPY_PRSPY_CNY              as country_region,
        CDSC_SALPHASE               as sales_phase_id,
        TDSC_SALPHASE               as sales_phase,
        CLEAD_DBA_SALESORG          as sales_organization_id,
        TLEAD_DBA_SALESORG          as sales_organization,
        COPP_ID                     as opportunity_id,
        TOPP_ID                     as opportunity,
        CQUOH_UUID                  as sales_quote_id,
        TQUOH_UUID                  as sales_quote,
        KCCCO_DTV_GROSS_AMOUNT      as gross_amount,
        RCCCO_DTV_GROSS_AMOUNT      as gross_amount_currency,
        KCCCO_DTV_NET_AMOUNT        as net_amount,
        RCCCO_DTV_NET_AMOUNT        as net_amount_currency,
        KCCCO_DTV_TAX_AMOUNT        as tax_amount,
        RCCCO_DTV_TAX_AMOUNT        as tax_amount_currency,
        KCDSF_EXPNETAMTRC           as item_value_of_opportunities,
        UCDSF_EXPNETAMTRC           as item_value_of_opportunities_currency,
        KCDSF_EXREVAMTRC            as expected_value,
        UCDSF_EXREVAMTRC            as expected_value_currency,
        KCDSF_PROB_PERCDC           as probability,
        KCDSF_WGEXNTAMTRC           as weighted_value,
        UCDSF_WGEXNTAMTRC           as weighted_value_currency,
        KCITV_NET_AMT_RC            as net_value,
        {{ dbt_date.from_unixtimestamp("changed_dt_epoch") }} as changed_dt,
        current_timestamp()         as meta_insert_ts  
    from date_trim
)

select * from renamed
