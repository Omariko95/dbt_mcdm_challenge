with facebook as (
    select 
    __insert_date,
    ad_id,
    add_to_cart,
    adset_id,
    campaign_id,
    channel,
    clicks,
    comments,
    creative_id,
    creative_title,
    objective,
    buying_type,
    campaign_type, 
    creative_body,
    date,
    likes,
    shares,
    comments_2,
    views, 
    clicks_2,
    impressions,
    mobile_app_install,
    inline_link_clicks,
    purchase,
    complete_registration,
    purchase_value,
    shares_2,
    spend,
    purchase_2,
    views_2

    
    from {{ ref("src_ads_creative_facebook_all_data") }})


    select * from facebook