with twitterreport as (select * from {{ ref("src_promoted_tweets_twitter_all_data") }}) select * from twitterreport
