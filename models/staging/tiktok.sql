with
    tiktok as (

        select
        __insert_date,
        ad_id,
        add_to_cart,
        adgroup_id,
        campaign_id,
        channel,
        clicks,
        date,
        impressions,
        rt_installs,
        skan_app_install,
        ad_status,
        ad_text,
        purchase,
        registrations,
        spend,
        conversions,
        skan_conversion,
        video_views

    
    
    
     from {{ ref("src_ads_tiktok_ads_all_data") }})


     select * from tiktok
