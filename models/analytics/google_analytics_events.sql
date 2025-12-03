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

{%- set custom_summed_metrics = [] -%}
{%- for tuple in ta_firebase.get_event_parameter_tuples_for_rollup_metrics () -%}
    {# cm = custom metris #}
    {%- set mrt = tuple['metric_rollup_transformation'] -%}
    {%- set sfn = tuple['struct_field_name'] -%}
    {%- set rsfn = tuple['rollup_struct_field_name'] -%}
    {%- set _ = custom_summed_metrics.append({"agg": mrt|replace("##", "event_parameters." ~ sfn) ~ " as " ~ rsfn, "alias": rsfn}) -%}
{%- endfor -%}
{%- set miniColumnsToAlsoNil = [] -%}

WITH data as (
    SELECT    DATE(event_ts) as event_date
            , {{ ta_firebase.install_age_group("install_age") }} AS install_age_group
            , {{ ta_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, miniColumnsToAlsoNil, "", "") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("google_analytics_events_raw") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    GROUP BY 1,2 {% for n in range(3, 3 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT event_date
        , install_age_group
        , {{ ta_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
        , cnt
        , users
        {{ ", " if custom_summed_metrics|length > 0 else "" }}  {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data
