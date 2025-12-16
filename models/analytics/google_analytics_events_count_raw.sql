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
    project_id,
    dataset_id,
    ,platform
    ,user_id
    ,sum(if(event_name = 'user_engagement',1,0)) as user_engagement
FROM  {{ ref("google_analytics_events_raw") }}
WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
AND event_name in ('user_engagement')
GROUP by 1,2,3,4,5

