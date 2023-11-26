with src as (
    select
        session_key as `Session Key`,
        ga_session_id as `GA Session ID`,
        user_pseudo_id as `User Pseudo ID`,
        is_consented as `Is Consented`
    from {{ ref('dim_ga4_doms_session') }}
)

select * from src
