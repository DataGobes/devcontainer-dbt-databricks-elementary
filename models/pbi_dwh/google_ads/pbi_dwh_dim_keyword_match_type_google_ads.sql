
with src as (
    select 
    keyword_matchtype_key as `Keyword Match Type Key`,
    keyword_matchtype as `Keyword Match Type`

    from {{ ref('dim_keyword_match_type_google_ads')}})

select * from src