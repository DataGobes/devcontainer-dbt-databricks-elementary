with src as (
    select distinct
        click_info_key as `Click Info ID`,
        campaignid as `Campaign Id`,
        adgroupid as `Ad Group Id`,
        adnetworktype as `Ad Network Type`,
        creativeid as `Creative Id`,
        criteriaid as `Criteria Id`,
        criteriaparameters as `Criteria Parameters`,
        customerid as `Customer Id`,
        gclid as `Google Click Id`,
        isvideoad as `Is video Ad`,
        page_number as `Page Number`,
        slot as `Slot`
    from {{ ref('dim_doms_adwords_clickinfo_ua_bq') }}
)

select * from src
