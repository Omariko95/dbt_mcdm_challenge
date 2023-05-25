with
    bing as (select * from {{ ref("bing") }}),

    facebook as (select * from {{ ref("facebook") }}),

    tiktok as (select * from {{ ref("tiktok") }}),

    twitter as (select * from {{ ref("twitter") }}),


    final as (
        select
        bing.__insert_date as date,
        bing.ad_id as string ,
        bing.adset_id as string , 
        bing.campaign_id as string ,
        bing.channel as string ,
        bing.clicks as int64 ,
        bing.date as date ,
        bing.revenue as int64 ,
        bing.spend as int64 ,

        from bing
    )
    


select * from final