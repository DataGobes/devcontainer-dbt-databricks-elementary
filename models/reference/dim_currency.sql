select distinct Currency_code_ISO as Currency_Code, Currency_Name 
from {{ ref('dim_currency_per_country') }} order by 1