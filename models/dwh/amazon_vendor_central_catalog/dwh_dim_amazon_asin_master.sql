with vc_catalog as (

    select
        *,
        'enr_amazon_vendor_central_catalog' as meta_src_table
    from
        {{ ref('enr_amazon_vendor_central_catalog') }}
),

-- extract extra asins from factor_a data

raw_amz_fa_dsp_product_report_extra_asins as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['amazon_country', 'asin_code']) }} as amazon_product_key,
        asin_code,
        amazon_country,
        product_title,
        'Not Set' as manufacturer_code,
        parent_asin,
        'Not Set' as upc,
        'Not Set' as ean,
        'Not Set' as isbn_13,
        'Not Set' as model_number,
        brand_name as brand,
        'Not Set' as brand_code,
        'Not Set' as product_group,
        'Not Set' as release_date,
        'Not Set' as replenishment_category,
        'Not Set' as prep_instructions_required,
        'Not Set' as prep_instructions_vendor_state,
        current_timestamp() as meta_insert_ts,
        'raw_amz_fa_dsp_product_report' as meta_src_table,
        row_number() over (partition by concat(amazon_country, '~', asin_code) order by flg_product_name asc, day_date desc) as rn
    from (
        select
            trim(country_code) as amazon_country,
            trim(amazon_standard_id) as asin_code,
            trim(product_name) as product_title,
            trim(parent_asin) as parent_asin,
            trim(brand_name) as brand_name,
            case when product_name is null then 99 else 1 end as flg_product_name,
            max(day_date) as day_date
        from
            {{ source('src_amazon_factor_a', 'amz_fa_dsp_product_report') }}
        group by
            trim(country_code),
            trim(amazon_standard_id),
            trim(product_name),
            trim(parent_asin),
            trim(brand_name),
            case when product_name is null then 99 else 1 end
    )
),

-- filter on rn = 1, obtain distinct from dsp_product_report

raw_amz_fa_dsp_product_report_extra_asins_distinct as (

    select distinct * --except(rn)
    from raw_amz_fa_dsp_product_report_extra_asins
    where rn = 1 and amazon_product_key not in (select amazon_product_key from vc_catalog)

),

asins_vc_fa_dsp_product_report_combined as (

    select
        amazon_product_key,
        asin_code,
        amazon_country,
        product_title,
        manufacturer_code,
        parent_asin,
        upc,
        ean,
        isbn_13,
        model_number,
        brand,
        brand_code,
        product_group,
        release_date,
        replenishment_category,
        prep_instructions_required,
        prep_instructions_vendor_state,
        meta_insert_ts,
        meta_src_table
    from
        vc_catalog
    union
    select
        amazon_product_key,
        asin_code,
        amazon_country,
        product_title,
        manufacturer_code,
        parent_asin,
        upc,
        ean,
        isbn_13,
        model_number,
        brand,
        brand_code,
        product_group,
        release_date,
        replenishment_category,
        prep_instructions_required,
        prep_instructions_vendor_state,
        meta_insert_ts,
        meta_src_table
    from
        raw_amz_fa_dsp_product_report_extra_asins_distinct
),


-- Foreach amazon sp-api fact table we obtain the asin codes that are not present in the previous asins_vc_fa_dsp_product_report_combined

dwh_fct_amazon_net_pure_product_margin_report_extra_asins as (

    select
        amazon_product_key,
        amazon_country,
        asin_code
    from
        {{ ref('dwh_fct_amazon_net_pure_product_margin_report') }}
    where amazon_product_key not in (select amazon_product_key from asins_vc_fa_dsp_product_report_combined)

),

dwh_fct_amazon_sales_report_extra_asins as (

    select
        amazon_product_key,
        amazon_country,
        asin_code
    from
        {{ ref('dwh_fct_amazon_sales_report') }}
    where amazon_product_key not in (select amazon_product_key from asins_vc_fa_dsp_product_report_combined)

),

dwh_fct_amazon_traffic_report_extra_asins as (

    select
        amazon_product_key,
        amazon_country,
        asin_code
    from
        {{ ref('dwh_fct_amazon_traffic_report') }}
    where amazon_product_key not in (select amazon_product_key from asins_vc_fa_dsp_product_report_combined)

),

-- Combine all asins found in facts that are not in the enriched vendor central catalog

sp_api_fact_extra_asins as (

    select
        amazon_product_key,
        asin_code,
        amazon_country,
        'Not Set' as product_title,
        'Not Set' as manufacturer_code,
        'Not Set' as parent_asin,
        'Not Set' as upc,
        'Not Set' as ean,
        'Not Set' as isbn_13,
        'Not Set' as model_number,
        'Not Set' as brand,
        'Not Set' as brand_code,
        'Not Set' as product_group,
        'Not Set' as release_date,
        'Not Set' as replenishment_category,
        'Not Set' as prep_instructions_required,
        'Not Set' as prep_instructions_vendor_state,
        first(meta_src_table) as meta_src_table
    from
        (
            select
                *,
                'dwh_fct_amazon_net_pure_product_margin_report_extra_asins' as meta_src_table
            from dwh_fct_amazon_net_pure_product_margin_report_extra_asins
            union
            select
                *,
                'dwh_fct_amazon_sales_report_extra_asins' as meta_src_table
            from dwh_fct_amazon_sales_report_extra_asins
            union
            select
                *,
                'dwh_fct_amazon_sales_report_extra_asins' as meta_src_table
            from dwh_fct_amazon_traffic_report_extra_asins
        )
    group by --noqa: AM06
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),

-- Combine with existing asins to obtain full list of asins for dim

all_asins_combined as (

    select
        amazon_product_key,
        asin_code,
        amazon_country,
        product_title,
        manufacturer_code,
        parent_asin,
        upc,
        ean,
        isbn_13,
        model_number,
        brand,
        brand_code,
        product_group,
        release_date,
        replenishment_category,
        prep_instructions_required,
        prep_instructions_vendor_state,
        current_timestamp() as meta_insert_ts,
        meta_src_table
    from
        asins_vc_fa_dsp_product_report_combined
    union
    select
        amazon_product_key,
        asin_code,
        amazon_country,
        product_title,
        manufacturer_code,
        parent_asin,
        upc,
        ean,
        isbn_13,
        model_number,
        brand,
        brand_code,
        product_group,
        release_date,
        replenishment_category,
        prep_instructions_required,
        prep_instructions_vendor_state,
        current_timestamp() as meta_insert_ts,
        meta_src_table
    from
        sp_api_fact_extra_asins
)

select * from all_asins_combined

--all_asins_combined
/*

--select count(*) from unique_asins_per_country

--select meta_src, count(*)
--from unique_asins_per_country
--group by meta_src

-- 1278 3616 duplicates

*/
