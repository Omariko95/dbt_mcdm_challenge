with
    bing as (select * from {{ ref("src_ads_bing_all_data") }}),

    facebook as (select * from {{ ref("src_ads_creative_facebook_all_data") }}),

    tiktok as (select * from {{ ref("src_ads_tiktok_ads_all_data") }}),

    twitter as (select * from {{ ref("src_promoted_tweets_twitter_all_data") }}),

    cpcbing as (select sum(spend) / sum(clicks) as Bing from bing),

    cpcfacebook as (select sum(spend) / sum(clicks) as Facebook from facebook),

    cpctiktok as (select sum(spend) / sum(clicks) as TikTok_Ads from tiktok),

    cpctwitter as (select sum(spend) / sum(clicks) as Twitter from twitter)

select * from cpcbing , cpcfacebook , cpctiktok , cpctwitter
