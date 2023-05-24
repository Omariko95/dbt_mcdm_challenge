with
    report as (select * from {{ ref("src_ads_bing_all_data") }})


select *
from report
