{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
    cluster_by = ["event_name", "platform", "bundle_id"]
) }}


{%- set columnNamesEventDimensions = ["bundle_id", "reverse_bundle_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}

{%- set miniColumnsToIgnoreInGroupBy = ta_firebase.get_mini_columns_to_ignore_when_rolling_up() -%}

{%- set tmp_res = ta_firebase.get_filtered_columns_for_table("google_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

{%- set install_types = [
    "custom_event",
    "our_first_open",
    "first_open",
    "any_first_event"
] -%}

WITH data as (
    SELECT    DATE(event_ts) as event_date
            , DATE(install_ts) as install_date
            , project_id
            , dataset_id
            , install_age
            , {{ ta_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}
            , COUNT(1) AS cnt
            , COUNT(IF(has_post_install_events_after_10s, 1, NULL)) AS cnt_qualified
            , COUNT(DISTINCT user_pseudo_id) AS users
            , COUNT(DISTINCT IF(has_post_install_events_after_10s, user_pseudo_id, NULL)) AS users_qualified

        {% for install_type in install_types %}
            , COUNT(IF(user_pseudo_id_{{ install_type }}, 1, NULL)) AS cnt_{{ install_type }}
            , COUNT(IF(user_pseudo_id_{{ install_type }} AND has_post_install_events_after_10s, 1, NULL)) AS cnt_{{ install_type }}_qualified
            , COUNT(DISTINCT IF(user_pseudo_id_{{ install_type }}, user_pseudo_id, NULL)) AS users_{{ install_type }}
            , COUNT(DISTINCT IF(user_pseudo_id_{{ install_type }} AND has_post_install_events_after_10s, user_pseudo_id, NULL)) AS users_{{ install_type }}_qualified
        {% endfor %}

    FROM {{ ref("google_analytics_installs_raw") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    GROUP BY 1,2,3,4,5 {% for n in range(6, 6 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT  event_date
      , install_date
      , project_id
      , dataset_id
      , install_age
      , {{ ta_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
      , cnt
      , cnt_qualified
      , users
      , users_qualified
{% for install_type in install_types %}
      , cnt_{{ install_type }}
      , cnt_{{ install_type }}_qualified
      , users_{{ install_type }}
      , users_{{ install_type }}_qualified
{% endfor %}
FROM data