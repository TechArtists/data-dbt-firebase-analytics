{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
    cluster_by = ["platform", "bundle_id"]
) }}
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false

 
WITH  custom_install_event AS (
        SELECT * FROM {{ ref('google_analytics_events_raw') }} 
        WHERE {{ ta_firebase.analyticsDateFilterFor('event_date',extend = 2) }}
          AND {% if var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT", "")|length > 0 -%}
                  event_name = '{{ var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT", "") }}'
              {%- else -%}
                  False
              {%- endif %}
          AND event_ts BETWEEN install_ts AND TIMESTAMP_ADD(install_ts, INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, ta_install_event AS (
        SELECT * FROM {{ ref('google_analytics_events_raw') }} 
        WHERE event_name = 'our_first_open' 
          AND {{ ta_firebase.analyticsDateFilterFor('event_date',extend = 2) }}
          AND event_ts BETWEEN install_ts AND TIMESTAMP_ADD(install_ts, INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, ga_install_event AS (
        SELECT * FROM {{ ref('google_analytics_events_raw') }} 
        WHERE event_name = 'first_open' 
          AND {{ ta_firebase.analyticsDateFilterFor('event_date', extend = 2) }}
          AND event_ts BETWEEN install_ts AND TIMESTAMP_ADD(install_ts, INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, any_first_event AS (
        SELECT * FROM {{ ref('google_analytics_events_raw') }} 
        WHERE True 
          AND {{ ta_firebase.analyticsDateFilterFor('event_date', extend = 2) }}
          AND event_ts BETWEEN install_ts AND TIMESTAMP_ADD(install_ts, INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, first_user_session_span AS (
    SELECT
        user_pseudo_id,
        TIMESTAMP_DIFF(
            MAX(event_ts),
            MIN(event_ts),
            SECOND
        ) AS seconds_in_app_on_install_day
    FROM {{ ref('google_analytics_events_raw') }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date', extend = 2) }}
      AND event_ts BETWEEN install_ts AND TIMESTAMP_ADD(install_ts, INTERVAL 1 DAY)
    GROUP BY user_pseudo_id
)
, user_pseudo_id_to_user_id AS (
        SELECT user_pseudo_id, user_id
        FROM {{ ref('google_analytics_events_raw') }} WHERE user_id IS NOT NULL AND {{ ta_firebase.analyticsDateFilterFor('event_date', extend = 2) }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)

{%- set miniColumnsToIgnoreInGroupBy = ["duplicates_cnt"] -%}
{%- set columns = ta_firebase.get_filtered_columns_for_table("google_analytics_events_raw", "*", miniColumnsToIgnoreInGroupBy)[0] -%}

{%- set columnsInSelect = [] -%}
{%- for column in columns %}
    {%- if column.name == 'user_id' -%}
        {%- set _ = columnsInSelect.append("users.user_id") -%}
    {%- else -%}
        {%- set _ = columnsInSelect.append("COALESCE(custom." ~ column.name ~ ", ta." ~ column.name ~ ", ga." ~ column.name ~ ", anyFirstEvent." ~ column.name ~ ") as " ~ column.name) -%}
    {%- endif -%}
{%- endfor %}


, data as (
    SELECT   {{ columnsInSelect | join("\n           , ") }}
             , custom.user_pseudo_id IS NOT NULL        AS user_pseudo_id_custom_event
             , ta.user_pseudo_id IS NOT NULL            AS user_pseudo_id_our_first_open
             , ga.user_pseudo_id IS NOT NULL            AS user_pseudo_id_first_open
             , anyFirstEvent.user_pseudo_id IS NOT NULL AS user_pseudo_id_any_first_event
             , span.seconds_in_app_on_install_day
             , span.seconds_in_app_on_install_day >= {{ var("TA:QUALIFIED_INSTALL_MIN_SECONDS", 10) }} AS has_post_install_events_after_10s
    FROM any_first_event as anyFirstEvent
    FULL OUTER JOIN ga_install_event as ga ON anyFirstEvent.user_pseudo_id = ga.user_pseudo_id
    FULL OUTER JOIN ta_install_event as ta ON anyFirstEvent.user_pseudo_id = ta.user_pseudo_id
    FULL OUTER JOIN custom_install_event as custom ON anyFirstEvent.user_pseudo_id = custom.user_pseudo_id
    LEFT JOIN user_pseudo_id_to_user_id as users ON COALESCE(anyFirstEvent.user_pseudo_id, ga.user_pseudo_id, ta.user_pseudo_id, custom.user_pseudo_id) = users.user_pseudo_id
    LEFT JOIN first_user_session_span as span    ON COALESCE(anyFirstEvent.user_pseudo_id, ga.user_pseudo_id, ta.user_pseudo_id, custom.user_pseudo_id) = span.user_pseudo_id
    WHERE True 
)
-- SELECT  COUNT(1) , COUNT(DISTINCT(user_pseudo_id)) 
SELECT *
FROM data 
WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}


