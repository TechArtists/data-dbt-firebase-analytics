{{ config(
    severity =  'error'
) }}

WITH stg AS (
SELECT event_date,project_id, SUM(duplicates_cnt) AS cnt FROM {{ ref('fb_analytics_events_raw') }} 
WHERE {{ overbase_firebase.analyticsTestDateFilter('event_date',extend=2) }}
and event_date <= current_date -5
GROUP BY 1,2
)
, src AS (

{% set projects = var('OVERBASE:SOURCES', []) %}

{% for p in projects %}
  {% if not loop.first %}UNION ALL{% endif %}
    SELECT DATE(TIMESTAMP_MICROS(event_timestamp)) as event_date,COUNT(*) AS cnt,
    '{{ p.project_id }}' as project_id,
  from {{ source('firebase_analytics__' ~ p.project_id, 'events') }}
  WHERE {{ overbase_firebase.analyticsTestTableSuffixFilter(extend = 3) }}
  AND {{ overbase_firebase.analyticsTestDateFilter('DATE(TIMESTAMP_MICROS(event_timestamp))',extend=2) }}
  AND DATE(TIMESTAMP_MICROS(event_timestamp)) <= current_date -5 --buffer because firebase keeps refreshing the recent partitions
GROUP BY 1,2
{% endfor %}
)
select * from 
stg left join src on stg.event_date = src.event_date and src.project_id=stg.project_id
where stg.cnt <> src.cnt
