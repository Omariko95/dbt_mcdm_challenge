with tiktokreport as (select * from {{ ref("src_ads_tiktok_ads_all_data") }}) select * from tiktokreport
