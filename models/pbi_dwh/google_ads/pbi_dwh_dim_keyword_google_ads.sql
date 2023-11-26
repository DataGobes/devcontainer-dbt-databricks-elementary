
with src as (
select 
keyword_key  as `Keyword Key`,
keyword_text as `Keyword Text`

from {{ ref('dim_keyword_google_ads')}})

select * from src