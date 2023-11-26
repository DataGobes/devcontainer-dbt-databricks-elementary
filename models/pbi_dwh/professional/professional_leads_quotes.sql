with leads_funnel as (
    select *
    from {{ ref('sap_c4c_report_lead_funnel') }}
),

quotes as (
    select distinct
        lead_id,
        sales_quote_id,
        sales_quote
    from leads_funnel
    where sales_quote_id != ''
),

renamed as (
    select 
        lead_id as `Lead ID`,
        sales_quote_id as `Sales Quote ID`,
        sales_quote as `Sales Quote`
    from quotes
)

select * from renamed
