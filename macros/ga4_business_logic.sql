-- Macro to select the base_url from a url string. Defined as the url without parameters
-- input: column containing a url
-- ouput: url without parameters
{% macro base_url(column_name) %}
    case 
        when CHARINDEX('?', {{column_name}}) != 0 and CHARINDEX('#', {{column_name}}) = 0  
        then LEFT({{column_name}}, CHARINDEX('?', {{column_name}}) -1) 
        when CHARINDEX('#', {{column_name}}) != 0 and CHARINDEX('?', {{column_name}})  = 0  
        then LEFT({{column_name}}, CHARINDEX('#', {{column_name}}) -1)
        when CHARINDEX('#', {{column_name}}) != 0 and CHARINDEX('?', {{column_name}}) != 0 and (CHARINDEX('#', {{column_name}}) < CHARINDEX('?', {{column_name}})) 
        then LEFT({{column_name}}, CHARINDEX('#', {{column_name}}) -1) 
        when CHARINDEX('#', {{column_name}}) != 0 and CHARINDEX('?', {{column_name}}) != 0 and (CHARINDEX('#', {{column_name}}) > CHARINDEX('?', {{column_name}})) 
        then LEFT({{column_name}}, CHARINDEX('?', {{column_name}}) -1)
        else {{column_name}} 
    end
{% endmacro %}

-- Macro to select the channel grouping based on the source/medium
-- input: traffic_source and traffic_medium
-- ouput: string with the value for the custom channel grouping

{% macro custom_channel_grouping(source, medium) %}
    case 
        when {{ medium }} like '%video-paid%' then 'Paid Video'
        when {{ medium }} = 'email' then 'Email'
        when {{ medium }} = 'affiliate' then 'Affiliates'
        when {{ medium }} = 'organic' then 'Organic Search'
        when {{ medium }} = 'social-paid' then 'Social Paid'
        when {{ medium }} = 'affiliate' then 'Affiliates'
        when {{ medium }} in ('display','cpm','banner')  then 'Display'
        when {{ medium }} in ('cpc','ppc','paidsearch')  then 'Paid Search'
        when {{ medium }} in ('social','social-network','social-media','sm','social network','social media')  then 'Social Organic'
        when {{ source }} regexp '(.*)(miele|miele_app|mieleexperience|miele-b2b|mieleforlife)\..{2,6}$'  then 'Miele Referral'
        when {{ medium }} = 'referral' then 'Referral'
        when {{ medium }} = '(direct)' and {{ source }} = '(none)' then 'Direct'
        else 'Other'
    end
{% endmacro %}