with lead_funnel as (
    select *
    from {{ ref('sap_c4c_report_lead_funnel') }}
),

lead_overview as (
    select *
    from {{ ref('sap_c4c_lead_overview') }}
),

fx_rates as (
    select *
    from {{ source('src_reference', 'dim_fx_rate') }}
),

agg_lead_funnel as (
    select
        lead_id,
        sales_phase,
        country_region_id,
        country_region,
        expected_value_currency,
        weighted_value_currency,
        cast(max(expected_value) over (partition by lead_id) as double) as expected_value,
        cast(max(weighted_value) over (partition by lead_id) as double) as weighted_value,
        row_number() over (partition by lead_id order by sales_order desc, sales_quote_id desc, opportunity_id desc) as rn
    from lead_funnel
),

date_keys as (
    select
        *,
        cast(replace(substring(start_date,1,10), '-', '') as int) as start_date_key,
        cast(replace(substring(end_date,1,10), '-', '') as int) as end_date_key,
        cast(replace(substring(start_date,1,7), '-', '') as int) as month_year
    from lead_overview
),

join_overview_funnel as (
    select
        overview.*,
        funnel.sales_phase,
        funnel.expected_value,
        funnel.expected_value_currency,
        funnel.weighted_value,
        funnel.weighted_value_currency,
        funnel.country_region_id,
        funnel.country_region
    from date_keys as overview
    left outer join agg_lead_funnel as funnel
        on overview.lead_id = funnel.lead_id
        and funnel.rn = 1
),

add_currency_conversion as (
    select
        leads.*,
        -- case statement to fill records where amount = 0 but no currency provided
        case when leads.expected_value = 0
             then 0
             else {{ convert_to_eur('leads.expected_value', 'fx_expected.fx_rate') }} 
        end as expected_value_eur,
        case when leads.weighted_value = 0
             then 0
             else {{ convert_to_eur('leads.weighted_value', 'fx_weighted.fx_rate') }} 
        end as weighted_value_eur
    from join_overview_funnel as leads
    left outer join fx_rates as fx_expected
        on leads.expected_value_currency = fx_expected.from_curr 
        and leads.month_year = fx_expected.month_year
    left outer join fx_rates as fx_weighted
        on leads.weighted_value_currency = fx_weighted.from_curr 
        and leads.month_year = fx_weighted.month_year
),

renamed as (
    select
        lead_id as `Lead ID`,
        lead_description as `Lead Description`,
        start_date as `Start Date`,
        start_date_key as `Start Date Key`,
        end_date as `End Date`,
        end_date_key as `End Date Key`,
        lifecycle_status as `Lifecycle Status`,
        lead_status as `Lead Status`,
        sales_phase as `Sales Phase`,
        campaign_id as `Campaign ID`,
        campaign as `Campaign`,
        source as `Source`,
        account_id as `Account ID`,
        account as `Account`,
        sales_organization_text as `Sales Organization`,
        expected_value as `Expected Value`,
        expected_value_eur as `Expected Value in Euro`,
        weighted_value as `Weighted Value`,
        weighted_value_eur as `Weighted Value in Euro`,
        country_region_id as `Country ID`,
        country_region as `Country`
    from add_currency_conversion
)

select * from renamed
