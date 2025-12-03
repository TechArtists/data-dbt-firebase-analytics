{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
     incremental_strategy = 'insert_overwrite',
     require_partition_filter = true
) }}


SELECT *
FROM {{ ref("google_analytics_events") }}
WHERE True 
AND  {{ ta_firebase.analyticsDateFilterFor('event_date') }}
AND (event_name LIKE 'error_%' OR event_name LIKE 'ta_error_%')