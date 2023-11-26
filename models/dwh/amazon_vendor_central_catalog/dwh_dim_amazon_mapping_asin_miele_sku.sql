-- Detailed explanation: https://miele365.sharepoint.com/:p:/r/sites/DataAnalytics/_layouts/15/Doc.aspx?sourcedoc=%7BC09C552D-546E-4DA9-9D6E-1A1BB135314E%7D&file=Amazon%20Product%20Mapping.pptx&_DSL=2&action=edit&mobileredirect=true

--sources

with direct_miele_sku as (
    select *
    from {{ source('src_amazon_asin_mapping', 'amazon_miele_sku_asin_mapping') }}
),

-- Get from the amazon asin master the entire list of asins with their details.
-- 2 priority flags are created: 
-- the first on the original source of the data, where the amazon vendor central catalog takes highest priority
-- second on the country code, but this is just a convention - it can be changed at any moment in time

prep01 as (
    select
        amazon_product_key,
        amazon_country as amz_country,
        asin_code,
        product_title as asin_desc,
        ean as ean_code,
        upc as upc_code,
        '2050-12-31' as max_transaction_date,
        case
            when meta_src_table = 'enr_amazon_vendor_central_catalog' then 1
            when meta_src_table = 'raw_amz_fa_dsp_product_report' then 2
            when meta_src_table = 'dwh_fct_amazon_sales_report_extra_asins' then 3
        end as flg_source_priority,
        case
            when amazon_country = 'DE' then 1
            when amazon_country = 'UK' then 2
            when amazon_country = 'IT' then 3
            when amazon_country = 'FR' then 4
            when amazon_country = 'US' then 5
            when amazon_country = 'ES' then 6
            else 99
        end as flg_amz_country
    from {{ ref('dwh_dim_amazon_asin_master') }}
),

-- Prepare the unique miele product master records per EAN codes to try the first matching criteria (by EAN code).
-- In case of duplicated SKUs for the same EAN choose the one that have a valid category, product group, subgroup or order by name
prep02 as (
    select
        ean_upc_code,
        dim_product_master_key,
        product_master_category,
        corporate_product_group,
        corporate_product_sub_group,
        segment_information,
        corporate_product_name
    from
        (
            select
                ean_upc_code,
                dim_product_master_key,
                product_master_category,
                corporate_product_group,
                corporate_product_sub_group,
                segment_information,
                corporate_product_name,
                row_number() over (partition by ean_upc_code order by flg_segm, flg_ctg, flg_grp, flg_subgrp, corporate_product_name) as rn
            from
                (
                    select
                        dim_product_master_key,
                        product_master_category,
                        corporate_product_group,
                        corporate_product_sub_group,
                        segment_information,
                        corporate_product_name,
                        trim(ean_upc_code) as ean_upc_code,
                        case when segment_information = 'Domestic' then 1 else 2 end as flg_segm,
                        case when product_master_category in ('Others', 'Not Set') then 2 else 1 end as flg_ctg,
                        case when corporate_product_group in ('Others', 'Not Set') then 2 else 1 end as flg_grp,
                        case when corporate_product_sub_group in ('Others', 'Not Set') then 2 else 1 end as flg_subgrp
                    from
                        dwh.dim_product_master_pim
                    where
                        ean_upc_code != 'Not Set'
                ) as q1
        ) as q2
    where
        rn = 1
),

-- First priority match on the direct Miele SKU provided by VGs
prio1_direct_miele_sku as (
    select
        a.amazon_product_key,
        a.amz_country,
        a.asin_code,
        a.asin_desc,
        a.ean_code,
        a.upc_code,
        a.max_transaction_date,
        a.flg_source_priority,
        a.flg_amz_country,
        b.miele_sku
    from prep01 as a
    left join direct_miele_sku as b
        on
            a.asin_code = b.asin_code
            and a.amz_country = b.amazon_country
),

-- Create a list of the mapped ASINs with the Miele SKUs.
-- Then the ean code in the amazon asin master.
-- Then the upc code in the amazon asin master.
prep03 as (

    select
        amazon_product_key,
        amz_country,
        asin_code as amz_asin_code,
        asin_desc as amz_asin_desc,
        ean_code as amz_ean_code,
        upc_code as amz_upc_code,
        miele_sku,
        max_transaction_date as amz_max_transaction_date,
        flg_source_priority as amz_flg_source_priority,
        flg_amz_country as amz_flg_amz_country,
        coalesce(coalesce(ean_upc_code_q, miele_ean_code_b, miele_upc_code_c), 'Not Set') as miele_ean_code,
        coalesce(coalesce(dim_product_master_key_q, dim_product_master_key_b, dim_product_master_key_c), '-1') as miele_product_master_key,
        coalesce(coalesce(product_master_category_q, product_master_category_b, product_master_category_c), 'Not Set') as product_master_category
    from
        (
            select
                a.amazon_product_key,
                a.amz_country,
                a.asin_code,
                a.asin_desc,
                a.ean_code,
                a.upc_code,
                a.miele_sku,
                q.ean_upc_code as ean_upc_code_q,
                q.dim_product_master_key as dim_product_master_key_q,
                q.product_master_category as product_master_category_q,
                a.max_transaction_date,
                a.flg_source_priority,
                a.flg_amz_country,
                b.dim_product_master_key as dim_product_master_key_b,
                b.product_master_category as product_master_category_b,
                c.dim_product_master_key as dim_product_master_key_c,
                c.product_master_category as product_master_category_c,
                trim(b.ean_upc_code) as miele_ean_code_b,
                trim(c.ean_upc_code) as miele_upc_code_c
            from prio1_direct_miele_sku as a
            left join (
                select distinct
                    dim_product_master_key,
                    product_master_category,
                    trim(ean_upc_code) as ean_upc_code
                from
                    dwh.dim_product_master_pim
                where
                    dim_product_master_key != '-1'
            ) as q
                on
                    a.miele_sku = q.dim_product_master_key
            left join
                prep02 as b
                on
                    a.ean_code = b.ean_upc_code
            left join
                prep02 as c
                on
                    a.upc_code = c.ean_upc_code
        ) as q
),

-- Create a list of asin codes that are mapped against a Miele SKU.
-- Here the amazon country is not relevant, we just need this dataset as the entire set of ASINs that could be mapped against a Miele SKU, regardless of the country.
prep04 as (
    select distinct
        amz_asin_code,
        miele_ean_code,
        miele_product_master_key,
        product_master_category,
        amz_flg_source_priority,
        amz_flg_amz_country,
        row_number() over (partition by amz_asin_code order by amz_flg_source_priority, amz_flg_amz_country) as rn
    from
        (
            select distinct
                amz_asin_code,
                miele_ean_code,
                miele_product_master_key,
                product_master_category,
                amz_flg_source_priority,
                amz_flg_amz_country
            from
                prep03
            where
                miele_product_master_key != '-1'
        )
),


-- In this dataset, we will check for every asin code if any of them (since prep3 contains same asin_code multiple times) is mapped against a Miele SKU.
-- If that's the case we will use this association to the other instances of the ASIN Key with the same asin code, but which could not be mapped initially.
-- e.g. select * from prep03 where amz_asin_code = 'B077K3YJFF'
-- Only one record has a valid Miele SKU and the other 4 don't; in this case the other 4 records will receive the same Miele SKU by association, 
-- but will preserve their official description which is different from country to country.
prep05 as (
    select
        a.amazon_product_key,
        a.amz_country,
        a.amz_asin_code,
        a.amz_asin_desc,
        a.amz_ean_code,
        a.amz_upc_code,
        a.amz_flg_source_priority,
        a.amz_max_transaction_date,
        b.miele_ean_code as b_miele_ean_code,
        b.miele_product_master_key as b_miele_product_master_key,
        case
            when (a.miele_ean_code is null or a.miele_ean_code = 'Not Set') and b.miele_ean_code is not null and b.miele_ean_code != 'Not Set' then b.miele_ean_code
            else a.miele_ean_code
        end as miele_ean_code,
        case
            when (a.miele_product_master_key is null or a.miele_product_master_key = '-1') and b.miele_product_master_key is not null and b.miele_product_master_key != '-1' then b.miele_product_master_key
            else a.miele_product_master_key
        end as miele_product_master_key,
        case
            when (a.product_master_category is null or a.product_master_category = 'Not Set') and b.product_master_category is not null and b.product_master_category != 'Not Set' then b.product_master_category
            else a.product_master_category
        end as product_master_category
    from prep03 as a
    left join
        ( --noqa:ST05
            select distinct
                amz_asin_code,
                miele_ean_code,
                miele_product_master_key,
                product_master_category
            from
                prep04
            where
                rn = 1
        ) as b
        on a.amz_asin_code = b.amz_asin_code
),

-- This is the dataset of ASINs that can be mapped based on EAN code or UPC code.
-- These ASIN codes will always be mapped with only one Miele SKU.
-- This will be called DATASET1.
prep06 as (
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        miele_ean_code,
        miele_product_master_key,
        product_master_category,
        row_number() over (partition by amazon_product_key order by amz_flg_source_priority asc, amz_max_transaction_date desc) as rn
    from
        prep05
    where
        miele_product_master_key != '-1'
),

-- This table will focus on all ASINs that cannot be mapped based on the EAN/UPC code (it doesn't exist or not a valid code for Miele, see where clause).
-- And therefore further will be checked for each ASIN code + ASIN description by checking all numbers within the ASIN description for a match with Miele SKU.
-- On the first column there will be an array/list containing any numeric value in the description of the ASIN code.
prep07 as (
    select
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        miele_ean_code,
        miele_product_master_key,
        regexp_extract_all(amz_asin_desc, '(\\d+)') as amz_asin_desc_list_of_numeric_strings
    from
        prep05
    where
        amz_asin_code not in
        --not in DATASET1 / prep06
        (
            select amz_asin_code
            from
                prep06
        )
),

-- This will be the dataset of the ASIN with a ASIN description that contains no numeric values, so it cannot be mapped to a Miele SKU.
-- But the same ASIN may have another description with numerics in it, so it will be re-joined together after the explode (next temp table).
-- This dataset is separated before the explode because otherwise the explode will filter it out.
prep08 as (
    select distinct
        '-1' as potential_sku,
        '-1' as potential_sku_raw,
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        miele_ean_code,
        miele_product_master_key
    from
        prep07
    where
        element_at(amz_asin_desc_list_of_numeric_strings, 1) is null
),

-- This will be the dataset of the ASIN with a "amz_asin_desc_list_of_numeric_strings" that contains at least a numeric value, so it has the potential to be mapped to a Miele SKU.
-- First the list of numeric is exploded to have it on the separate rows to be later joined with the list of Miele SKUs.
-- Then each number extracted will be padded with leading zero's to the maximum of 8 digits which is the length of the Miele SKUs.
prep09 as (
    select distinct
        potential_sku_raw,
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        miele_ean_code,
        miele_product_master_key,
        lpad(trim(potential_sku_raw), 8, '0') as potential_sku
    from
        (
            select
                amazon_product_key,
                amz_country,
                amz_asin_code,
                amz_asin_desc,
                amz_ean_code,
                amz_upc_code,
                amz_max_transaction_date,
                miele_ean_code,
                miele_product_master_key,
                explode(amz_asin_desc_list_of_numeric_strings) as potential_sku_raw
            from
                prep07
            where
                element_at(amz_asin_desc_list_of_numeric_strings, 1) is not null
        ) as q
),

-- Combine the prep08 and prep09 to continue the further processing.
-- This will contain all the ASINs that are not mapped to a Miele SKU:
----1. Have no digit in the description so will never be mapped
----2. Have at least a digit in the description that could be mapped to a Miele SKU
prep10 as (
    select distinct
        potential_sku, -- '-1' 
        potential_sku_raw, -- '-1'
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date
    from
        prep08
    union all
    select distinct
        potential_sku,
        potential_sku_raw,
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date
    from
        prep09
),

-- Create a dataset that will add to each Miele SKU some flag columns that will classify later the SKUs for the same ASINs.
-- e.g. if one ASIN could be mapped to 2 Miele SKUs where SKU1 has category "Others" and SKU2 has category "A&C", then SKU2 will be chosen
-- Priority has 1. segment (Domestic), 2. category (non not set or non-Others) and 3. Product Group (non not set or non-Others) and 4. brand (Miele)
prep11 as (

    select
        dim_product_master_id,
        prod_master_name,
        corporate_article_name,
        ean_upc_code,
        product_master_category,
        corporate_product_group,
        segment_information,
        product_master_brand,
        flg_segment,
        flg_category,
        flg_prdgrp,
        flg_brand,
        1000 * flg_segment + 100 * flg_category + 10 * flg_prdgrp + flg_brand as computed_rank_flg
    from
        (
            select
                dim_product_master_id,
                prod_master_name,
                corporate_article_name,
                ean_upc_code,
                product_master_category,
                corporate_product_group,
                segment_information,
                product_master_brand,
                case
                    when lower(segment_information) = 'domestic' then 1
                    else 2
                end as flg_segment,
                case
                    when lower(product_master_category) in ('a&c', 'spare part') then 1
                    when lower(product_master_category) in ('not set', 'others') then 3
                    else 2
                end as flg_category,
                case
                    when lower(corporate_product_group) in ('accessories vacuum cleaners', 'machine care', 'accessories cooker hoods', 'accessories washing machines, tumble dryers and ironers', 'miele detergents', 'miele dishwasher detergents') then 1
                    when corporate_product_group in ('Not Set', 'Others') then 3
                    else 2
                end as flg_prdgrp,
                case
                    when product_master_brand in ('Miele') then 1
                    else 2
                end as flg_brand
            from
                dwh.dim_product_master_pim
        ) as q
),

-- This dataset will contain for each record in the prep11 table the corresponding Miele SKU for each value in the potential_sku column 
-- However, for each ASINs the name may contain multiple potential Miele SKUs so in the end we will need to map it to just one.
-- e.g. ASIN = B09N69CHR6 for FR  
-- This will be called DATASET X
prep12 as (

    select distinct
        q.*,
        q.amazon_product_key as prod_key,
        q.amz_asin_desc as amz_name,
        row_number() over (partition by q.amazon_product_key order by q.flg_sku asc, q.computed_rank_flg asc, coalesce(q.prod_master_name, 'a') asc, q.dim_product_master_id asc, q.amz_asin_desc asc, q.amz_max_transaction_date desc) as rn
    from
        ( -- all non mapped asins with a possible match in miele prod master--
            select distinct
                b.*,
                a.*,
                case
                    when b.dim_product_master_id is null then 99
                    else 1
                end as flg_sku
            from
                prep10 as a
            left join
                prep11 as b
                on
                    a.potential_sku = b.dim_product_master_id
        ) as q

),

-- This will be the dataset of the ASIN with a ASIN description that contains numeric values and they can be mapped against a Miele SKU
-- This is called DATASET2
prep13 as (
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        ean_upc_code as miele_ean_code,
        dim_product_master_id as miele_product_master_key
    from
        prep12
    where
        rn = 1
        and
        dim_product_master_id is not null
        and
        dim_product_master_id != '-1'
),

-- From DATASET X check on all the unmapped ASIN keys (with rn=1) if the same ASIN but for another country has been mapped to a valid Miele SKU (remember that this mapping is done based on the local description)
-- If similar ASIN received a valid mapping, reuse the same for all markets with the same ASIN code
-- However, keep in mind multiple records could be added to the final dataset for the same key, so a RN is required later to only extract one record per ASIN key
-- This query contains in the end two datasets:
---- 1. ASIN keys that were mapped through "contagion" of the same ASIN code (the ones with a valid miele_product_master_key)                        - this will be DATASET3 
---- 2. ASIN keys that could not be mapped, not even through "contagion" of the same ASIN code (the ones with an invalid miele_product_master_key)    - this will be DATASET4 (will remain unmapped)
prep14 as (
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_asin_desc,
        amz_ean_code,
        amz_upc_code,
        amz_max_transaction_date,
        miele_ean_code,
        miele_product_master_key
    from
        (
            select distinct
                amazon_product_key,
                amz_country,
                amz_asin_code,
                amz_asin_desc,
                amz_ean_code,
                amz_upc_code,
                amz_max_transaction_date,
                miele_ean_code,
                miele_product_master_key,
                row_number() over (partition by amazon_product_key order by amz_max_transaction_date desc, flg_ean asc) as rn
            from
                (
                    select distinct
                        a.amazon_product_key,
                        a.amz_country,
                        a.amz_asin_code,
                        a.amz_asin_desc,
                        a.amz_ean_code,
                        a.amz_upc_code,
                        a.amz_max_transaction_date,
                        b.miele_ean_code,
                        b.miele_product_master_key,
                        case
                            when b.miele_ean_code = 'Not Set' then 2
                            else 1
                        end as flg_ean
                    from
                        (
                            select distinct
                                amazon_product_key,
                                amz_country,
                                amz_asin_code,
                                amz_asin_desc,
                                amz_ean_code,
                                amz_upc_code,
                                amz_max_transaction_date
                            from
                                prep12
                            where
                                amazon_product_key not in (
                                    select amazon_product_key
                                    from
                                        prep13
                                )
                        ) as a
                    left join
                        prep13 as b
                        on
                            a.amz_asin_code = b.amz_asin_code
                ) as q
        ) as p
    where
        rn = 1
),

all_datasets as (

    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_ean_code,
        amz_upc_code,
        miele_ean_code,
        miele_product_master_key,
        'DATASET1' as source_flag
    from
        prep06 -- Mapped from the very beginning
    where
        rn = 1
    union all
    --this is DATASET2--
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_ean_code,
        amz_upc_code,
        miele_ean_code,
        miele_product_master_key,
        'DATASET2' as source_flag
    from
        prep13 -- Mapped from the numeric value (miele sku) in the asin description
    union all
    --this is DATASET3--
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_ean_code,
        amz_upc_code,
        miele_ean_code,
        miele_product_master_key,
        'DATASET3' as source_flag
    from
        prep14
    where
        miele_product_master_key is not null -- Mapped from the numeric value (miele sku) of the same asin description, but for another country
    union all
    --this is DATASET4--
    select distinct
        amazon_product_key,
        amz_country,
        amz_asin_code,
        amz_ean_code,
        amz_upc_code,
        'Not Set' as miele_ean_code,
        '-1' as miele_product_master_key,
        'DATASET4' as source_flag
    from
        prep14
    where
        miele_product_master_key is null -- Still not mapped to Miele SKU
)

select
    *,
    current_timestamp() as meta_insert_ts
from
    all_datasets
