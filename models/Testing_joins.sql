with
    bing as (select * from {{ ref("src_ads_bing_all_data") }}),

    facebook as (select * from {{ ref("src_ads_creative_facebook_all_data") }}),

    final as (
        select 
        bing.ad_id,
        bing.adset_id,
        bing.campaign_id,
        bing.channel,
        bing.clicks,
        bing.date,
        bing.revenue,
        bing.spend,
        bing.conv,

        facebook.ad_id,
        facebook.add_to_cart,
        facebook.adset_id,
        facebook.campaign_id,
        facebook.channel,
        facebook.clicks,
        facebook.comments,
        facebook.creative_id,
        facebook.date,
        facebook.impressions,
        facebook.likes,
        facebook.inline_link_clicks,
        facebook.purchase,
        facebook.complete_registration,
        facebook.shares,
        facebook.spend,
        facebook.views_2
         
         
        from bing , facebook
    )


select * from final