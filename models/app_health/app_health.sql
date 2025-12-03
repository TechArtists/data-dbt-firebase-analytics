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

{%- set custom_summed_measures = [] -%}
{# mini_measures": ["cnt", "users"] are implicit in all of them, as the default #}
{%- set builtinMeasures = [
  {"model": "analytics", "name":"user_engagement"  , "agg": "SUM(##)", "event_name": "user_engagement"},
  {"model": "analytics", "name":"ta_app_foreground", "agg": "SUM(##)", "event_name": "ta_app_foreground"},
  {"model": "analytics", "name":"ta_app_background", "agg": "SUM(##)", "event_name": "ta_app_background"},
  {"model": "analytics", "name":"app_update"   , "agg": "SUM(##)", "event_name": "app_update"},
  {"model": "analytics", "name":"ta_app_update", "agg": "SUM(##)", "event_name": "ta_app_update"},
  {"model": "analytics", "name":"errors"  , "agg": "SUM(##)", "event_name": "LIKE 'error_%'"},
  {"model": "analytics", "name":"ta_errors"  , "agg": "SUM(##)", "event_name": "LIKE 'ta_error_%'"},

  {"model": "crashlytics", "name":"crashlytics_all_errors", "agg": "SUM(##)"},
  {"model": "crashlytics", "name":"fatal_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL'"},
  {"model": "crashlytics", "name":"fatal_foreground_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL' AND process_state = 'FOREGROUND'"},
  {"model": "crashlytics", "name":"fatal_background_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL' AND process_state = 'BACKGROUND' "}
] %}
{%- set allUnprocessedHealthMeasures = builtinMeasures + var("TA:CUSTOM_APP_HEALTH_MEASURES", []) %}
{%- set allAnalyticsEventNames = set([]) -%}
{%- set allAnalyticsForcedNullEventNames = set([]) -%}
{%- for customHealthMeasure in allUnprocessedHealthMeasures -%}
    {%- set model = customHealthMeasure['model'] if customHealthMeasure['model'] is defined else "analytics" %}
    {%- if model not in ["analytics", "analytics_forced_nulls", "crashlytics"]-%}
        {{ exceptions.raise_compiler_error("Need to specify a valid model for each custom app health measure. Either 'analytics','analytics_forced_nulls', 'crashlytics'. Found " + model) }}
    {%- endif -%}
    {%- set user_column_name = customHealthMeasure['name'] %}
    {%- if " " in user_column_name -%}
        {{ exceptions.raise_compiler_error("Can't have spaces inside the name of a custom app health measure. It needs to be a valid column name." ) }}
    {%- endif -%}
    {%- set additional_filter = customHealthMeasure["additional_filter"] if customHealthMeasure["additional_filter"] is defined else "True" -%}
    {%- if customHealthMeasure["event_name"] is defined -%}
      {%- if customHealthMeasure["event_name"].lower().startswith("like ") -%}
        {%- set filter = "event_name " ~ customHealthMeasure["event_name"] -%}
      {%- else -%}
        {%- set filter = "event_name = '" ~ customHealthMeasure["event_name"] ~ "' AND " ~ additional_filter -%}
      {%- endif -%}

      {%- if model == 'analytics' -%}
        {%- set _ = allAnalyticsEventNames.add(customHealthMeasure["event_name"]) -%}
      {%- elif model == 'analytics_forced_nulls' -%}
        {%- set _ = allAnalyticsForcedNullEventNames.add(customHealthMeasure["event_name"]) -%}
      {%- endif -%}
    {%- else -%}
      {%- set filter = additional_filter -%}
    {%- endif -%}
    {%- set mini_measures = customHealthMeasure["mini_measures"] if customHealthMeasure["mini_measures"] is defined else ["cnt", "users"] -%}
    {%- for cnt in mini_measures -%}
        {%- set column_name = "" ~ user_column_name ~ "_" ~ cnt %}
        {%- set agg  = customHealthMeasure['agg'] | replace("##", "IF(" ~ filter    ~ ", " ~ cnt ~", 0)") -%}
        {%- set _ = custom_summed_measures.append({"model": model, "agg": agg ~ " as " ~ column_name, "alias": column_name }) -%}
    {%- endfor -%}
{%- endfor -%}

{%- set commonDimensions = ["event_date","platform","app_id","reverse_app_id",
                            "app_version.join_value","platform_version.join_value",
                            "device_hardware.type","device_hardware.manufacturer","device_hardware.os_model"] -%}

{%- set commonDimenionAliases = commonDimensions | map('replace', '.', '_') | list -%}
{%- set commonDimensionsAndAliases = zip(commonDimensions, commonDimenionAliases) | list -%}

WITH analytics AS (
    SELECT {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "," if not loop.first else "" }} {{ dimAndAlias[0] }} AS {{ dimAndAlias[1] }}
           {% endfor -%}
          , {{ custom_summed_measures | selectattr("model", "equalto", "analytics") | map(attribute='agg')|join("\n          , ") }}
    FROM {{ ref("google_analytics_events") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    AND {{ ta_firebase.makeListIntoSQLInFilter("event_name", allAnalyticsEventNames| list) }}
    GROUP BY {{ range(1, 1 + commonDimensionsAndAliases | length) | list | join(",") }} 
)
, analyticsForcedNulls AS (
    SELECT {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "," if not loop.first else "" }} {{ dimAndAlias[0] }} AS {{ dimAndAlias[1] }}
           {% endfor -%}
          {%- for measure in  custom_summed_measures | selectattr("model", "equalto", "analytics_forced_nulls") | map(attribute='agg') %}
            , {{ measure }}
          {%- endfor %}
    FROM {{ ref("google_analytics_events_forced_nulls") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    {% if allAnalyticsForcedNullEventNames | length == 0 -%}
    AND False
    {%- else -%}
    AND {{ ta_firebase.makeListIntoSQLInFilter("event_name", allAnalyticsForcedNullEventNames| list) }}
    {%- endif %}
    GROUP BY {{ range(1, 1 + commonDimensionsAndAliases | length) | list | join(",") }} 
)
, installs AS (
    SELECT {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "," if not loop.first else "" }} {{ dimAndAlias[0] }} AS {{ dimAndAlias[1] }}
           {% endfor -%}
          , SUM(users) as users
    FROM {{ ref("google_analytics_installs") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    GROUP BY {{ range(1, 1 + commonDimensionsAndAliases | length) | list | join(",") }} 
)
, crashlytics AS (
    SELECT {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "," if not loop.first else "" }} {{ dimAndAlias[0] }} AS {{ dimAndAlias[1] }}
           {% endfor -%}
            , {{ custom_summed_measures | selectattr("model", "equalto", "crashlytics") | map(attribute='agg')|join("\n          , ") }}
    FROM {{ ref("crashlytics_events") }}
    WHERE {{ ta_firebase.crashlyticsDateFilterFor('event_date') }}
    GROUP BY {{ range(1, 1 + commonDimensionsAndAliases | length) | list | join(",") }} 
)
, joined_unpacked AS (
    SELECT {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "," if not loop.first else "" }} COALESCE(analytics.{{ dimAndAlias[1] }}, analyticsForcedNulls.{{ dimAndAlias[1] }}, installs.{{ dimAndAlias[1] }}, crashlytics.{{ dimAndAlias[1] }}) AS {{ dimAndAlias[1] }}
           {% endfor -%}
           , installs.users as installs
           , {{ custom_summed_measures | map(attribute='alias') | join("\n          , ") }}
    FROM analytics
    FULL OUTER JOIN analyticsForcedNulls ON 
           {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "AND" if not loop.first else "" }} analytics.{{ dimAndAlias[1] }} = analyticsForcedNulls.{{ dimAndAlias[1] }}
           {% endfor %}
    FULL OUTER JOIN installs ON 
           {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "AND" if not loop.first else "" }} analytics.{{ dimAndAlias[1] }} = installs.{{ dimAndAlias[1] }}
           {% endfor %}
    FULL OUTER JOIN crashlytics ON 
          {%- for dimAndAlias in commonDimensionsAndAliases -%}
           {{ "AND" if not loop.first else "" }} analytics.{{ dimAndAlias[1] }} = crashlytics.{{ dimAndAlias[1] }}
           {% endfor %}
)
SELECT  event_date
      , platform 
      , app_id
      , reverse_app_id
      , {{ ta_firebase.get_version_record_from_normalized('app_version_join_value') }} AS app_version
      , {{ ta_firebase.get_version_record_from_normalized('platform_version_join_value') }} AS platform_version
      , STRUCT<type STRING, manufacturer STRING, os_model STRING>(
         device_hardware_type, device_hardware_manufacturer, device_hardware_os_model
      ) as device_hardware
      , installs
      , {{ ta_firebase.list_map_and_add_prefix(custom_summed_measures | map(attribute='alias')) | join("\n          ,") }}
     
FROM joined_unpacked

-- For debugging
-- SELECT    SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as both_not_null
--         , SUM(IF(user_engagement_cnt IS     NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as just_in_crashlytics
--         , SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS     NULL, 1, 0)) as just_in_analytics
--         , SUM(1) as total_cnt
-- FROM joined_unpacked
