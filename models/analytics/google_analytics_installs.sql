{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
    cluster_by = ["event_name", "platform", "app_id"]
) }}


{%- set columnNamesEventDimensions = ["app_id", "reverse_app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}

{%- set miniColumnsToIgnoreInGroupBy = ta_firebase.get_mini_columns_to_ignore_when_rolling_up() -%}

{%- set tmp_res = ta_firebase.get_filtered_columns_for_table("google_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

WITH data as (
    SELECT    DATE(event_ts) as event_date
            , DATE(install_ts) as install_date
            , install_age
            , {{ ta_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users

    FROM {{ ref("google_analytics_installs_raw") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    GROUP BY 1,2,3 {% for n in range(4, 4 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT  event_date
      , install_date
      , install_age
      , {{ ta_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
      , cnt
      , users
FROM data
