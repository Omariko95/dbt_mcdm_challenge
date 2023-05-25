with
    bing as (

        select
        __insert_date,
        ad_id ,
        adset_id,
        campaign_id,
        channel,
        ad_description,
        title_part_1,
        title_part_2,
        clicks,
        date,
        imps,
        revenue,
        spend,
        conv

    
     from {{ ref("src_ads_bing_all_data") }})


     select * from bing
