{{ config(
    severity =  'error'
) }}

WITH stg AS (
SELECT event_date, SUM(duplicates_cnt) AS cnt FROM {{ ref('fb_analytics_events_raw') }} 
WHERE {{ overbase_firebase.analyticsTestDateFilter('event_date',extend=2) }}
and event_date <= current_date -5
GROUP BY 1
)
, src AS (

SELECT DATE(TIMESTAMP_MICROS(event_timestamp)) as event_date,COUNT(*) AS cnt
FROM {{ source("firebase_analytics", "events") }}
WHERE {{ overbase_firebase.analyticsTestTableSuffixFilter(extend = 3) }}
AND {{ overbase_firebase.analyticsTestDateFilter('DATE(TIMESTAMP_MICROS(event_timestamp))',extend=2) }}
AND DATE(TIMESTAMP_MICROS(event_timestamp)) <= current_date -5 --buffer because firebase keeps refreshing the recent partitions
GROUP BY 1
)
select * from 
stg left join src on stg.event_date = src.event_date
where stg.cnt <> src.cnt
