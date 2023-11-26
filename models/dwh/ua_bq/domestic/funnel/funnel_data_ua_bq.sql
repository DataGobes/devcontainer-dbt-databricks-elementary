with dim_page as (
    select *
    from  {{ ref('dim_doms_page_ua_bq') }}
),

dim_event as (
    select *
    from  {{ ref('dim_doms_event_ua_bq') }}
),

fact_page_hit as (
    select *
    from  {{ ref('fact_doms_page_hit_ua_bq') }}
),

fact_event_hit as (
    select *
    from  {{ ref('fact_doms_event_hit_ua_bq') }}
),

union_funnel as (
    select  
        f.visit_date,
        f.view_id,
        f.session_key,
        f.hit_number,
        case when p.page_path in ('/','/nl/','/fr/','/de/','/it/') then 'home' else p.page_type end as step
    from fact_page_hit as f 
    inner join dim_page as p
        on f.page_key = p.page_key

    union all

    select   
        f.visit_date,
        f.view_id,
        f.session_key,
        f.hit_number,
        case when e.event_category = 'E-Commerce' and e.event_action = 'Add To Cart Click' then 'atc' else e.event_action end as step
    from fact_event_hit as f
    inner join dim_event as e 
        on f.event_key=e.event_key  
    where 
        f.visit_date >= date_format(date_add(current_date(), -600),'yyyyMMdd')
        and e.event_category = 'E-Commerce' and e.event_action = 'Add To Cart Click'
),

ordered as (
    select *
    from union_funnel
    order by session_key, hit_number
),

final as (
    select 
        visit_date, 
        view_id, 
        session_key,
        concat_ws(',',collect_list(step)) as funnel 
    from ordered 
    group by visit_date, view_id, session_key
)

select * from final
