with
    twitter as (

        select
        __insert_date,
        campaign_id,
        channel,
        url,
        text,
        clicks,
        comments,
        date,
        engagements,
        impressions,
        likes,
        url_clicks,
        retweets,
        spend,
        video_total_views

    
    
    
     from {{ ref("src_promoted_tweets_twitter_all_data") }})


     select * from twitter
