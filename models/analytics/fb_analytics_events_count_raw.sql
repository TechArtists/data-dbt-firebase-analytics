{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
) }}



SELECT
    event_date
    ,platform
    ,user_id
    ,sum(if(event_name = 'user_engagement',1,0)) as user_engagement
FROM  {{ ref("fb_analytics_events_raw") }}
WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
AND event_name in ('user_engagement')
GROUP by 1,2,3

