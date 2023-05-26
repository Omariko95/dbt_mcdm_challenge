-- Step1: IMPORTS
with
    bing as (select * from {{ ref("src_ads_bing_all_data") }}),

    facebook as (select * from {{ ref("src_ads_creative_facebook_all_data") }}),

    tiktok as (select * from {{ ref("src_ads_tiktok_ads_all_data") }}),

    twitter as (select * from {{ ref("src_promoted_tweets_twitter_all_data") }}),

    -- Step 2: Build CTE logic
    paid_ads__basic_performance as (

        select
            bing.date,
            null as add_to_cart,
            bing.clicks,
            null as comments,
            null as engagements,
            bing.imps as impressions,
            null as installs,
            null as likes,
            null as link_clicks,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as purchase,
            null as registrations,
            bing.revenue,
            null as shares,
            bing.spend,
            bing.conv as total_conversions,
            null as video_views,
            cast(bing.ad_id as string) as ad_id,
            cast(bing.adset_id as string) as adset_id,
            cast(bing.campaign_id as string) as campaign_id,
            cast(bing.channel as string) as channel,
            null as creative_id,
            null as placement_id

        from bing

        union all

        select
            facebook.date,
            facebook.add_to_cart,
            facebook.clicks,
            facebook.comments,
            facebook.views as engagements,
            facebook.impressions,
            null as installs,
            facebook.likes,
            facebook.inline_link_clicks as link_clicks,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            facebook.purchase,
            facebook.complete_registration as registrations,
            null as revenue,
            facebook.shares as shares,
            facebook.spend,
            null as total_conversions,
            null as video_views,
            cast(facebook.ad_id as string) as ad_id,
            cast(facebook.adset_id as string) as adset_id,
            cast(facebook.campaign_id as string) as campaign_id,
            cast(facebook.channel as string) as channel,
            cast(facebook.creative_id as string) as creative_id,
            null as placement_id

        from facebook

        union all

        select
            tiktok.date,
            tiktok.add_to_cart,
            tiktok.clicks,
            null as comments,
            null as engagements,
            tiktok.impressions,
            tiktok.rt_installs as installs,
            null as likes,
            null as link_clicks,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            tiktok.purchase,
            tiktok.registrations,
            null as revenue,
            null as shares,
            tiktok.spend,
            tiktok.conversions as total_conversions,
            tiktok.video_views,
            cast(tiktok.ad_id as string) as ad_id,
            null as adset_id,
            cast(tiktok.campaign_id as string) as campaign_id,
            cast(tiktok.channel as string) as channel,
            null as creative_id,
            null as placement_id

        from tiktok

        union all

        select
            twitter.date,
            null as add_to_cart,
            twitter.clicks,
            twitter.comments,
            twitter.engagements,
            twitter.impressions,
            null as installs,
            twitter.likes,
            twitter.url_clicks as link_clicks,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as purchase,
            null as registrations,
            null as revenue,
            null as shares,
            twitter.spend,
            null as total_conversions,
            twitter.video_total_views as video_views,
            null as ad_id,
            null as adset_id,
            cast(twitter.campaign_id as string) as campaign_id,
            cast(twitter.channel as string) as channel,
            null as creative_id,
            null as placement_id

        from twitter

    )

-- Step 3: Execute select from paid_ads__basic_performance
select

    date,
    add_to_cart,
    clicks,
    comments,
    engagements,
    impressions,
    installs,
    likes,
    link_clicks,
    post_click_conversions,
    post_view_conversions,
    posts,
    purchase,
    registrations,
    revenue,
    shares,
    spend,
    total_conversions,
    video_views,
    ad_id,
    adset_id,
    campaign_id,
    channel,
    creative_id,
    placement_id,

from paid_ads__basic_performance
