{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}
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

-- https://support.google.com/firebase/answer/7029846
SELECT    
          project_id,
          dataset_id,
          TIMESTAMP_MICROS(event_timestamp) as event_ts
        , DATE(TIMESTAMP_MICROS(event_timestamp)) as event_date
        , TIMESTAMP_MICROS(user_first_touch_timestamp) as install_ts
        , {{ overbase_firebase.calculate_age_between_timestamps("TIMESTAMP_MICROS(event_timestamp)", "TIMESTAMP_MICROS(user_first_touch_timestamp)") }} as install_age
        , LOWER(user_pseudo_id) as user_pseudo_id
        , LOWER(user_id) as user_id
        , app_info.id as app_id
        , ARRAY_TO_STRING(ARRAY_REVERSE(SPLIT(app_info.id, '.')), '.') as reverse_app_id
        , event_name
        , {{ var( 'OVERBASE:CUSTOM_PLATFORM_PREPROCESSOR', 'platform') }} as platform
        , app_info.install_source as appstore
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "app_info.version" -%}
            {{ v }}, {{ overbase_firebase.get_version(v, "major") }}, {{ overbase_firebase.get_version(v, "minor") }}, {{ overbase_firebase.get_version(v, "bugfix") }}, {{ overbase_firebase.get_version(v, "major.minor") }}, {{ overbase_firebase.get_version(v, "major.minor.bugfix") }}, {{ overbase_firebase.get_version(v, "normalized") }}, COALESCE(CAST({{ overbase_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS app_version
        # join_value: COALESCE(normalized, "almost" firebase_value). FB Analytics's firebase_value is 'iOS 16.7.1', but Crashlytics' is just '16.7.1', so use just '16.7.1'
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "REPLACE(device.operating_system_version, device.operating_system ||' ','')" -%}
            device.operating_system_version, {{ overbase_firebase.get_version(v, "major") }}, {{ overbase_firebase.get_version(v, "minor") }}, {{ overbase_firebase.get_version(v, "bugfix") }}, {{ overbase_firebase.get_version(v, "major.minor") }}, {{ overbase_firebase.get_version(v, "major.minor.bugfix") }}, {{ overbase_firebase.get_version(v, "normalized") }}, COALESCE(CAST( {{ overbase_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS platform_version
        , {{ overbase_firebase.generate_struct_for_raw_user_properties() }} as user_properties
        , {{ overbase_firebase.generate_struct_for_raw_event_parameters() }} as event_parameters
        , user_properties as user_properties_raw
        , event_params as event_parameters_raw
        , STRUCT<city STRING , country_firebase_value STRING, iso_country_name STRING , iso_country_alpha_2 STRING, continent STRING, subcontinent STRING, region STRING, metro STRING>(
            geo.city, geo.country, country_codes.name, country_codes.alpha_2, geo.continent, geo.sub_continent, geo.region, geo.metro
        ) as geo
        -- for iOS it's ../Apple/iPhone 14/NULL/iPhone14,7
        -- for Android it's ../Samsung/SM-A146U/Galaxy A14 5G/SM-A146U or ../Motorola/Moto G Power (2022)/NULL/moto g power (2022)
        , STRUCT<type STRING,manufacturer STRING,model_name STRING,marketing_name STRING,os_model STRING>(
            LOWER(device.category), LOWER(device.mobile_brand_name), device.mobile_model_name, device.mobile_marketing_name, LOWER(device.mobile_os_hardware_model) 
        ) AS device_hardware
        , STRUCT<firebase_value STRING, iso_language_alpha_2 STRING, iso_country_alpha_2 STRING>(
            device.language, language_codes.alpha_2, language_region_codes.alpha_2
        ) AS device_language
        , IF(device.time_zone_offset_seconds >= 0,'+', '-') || LEFT(CAST(TIME(TIMESTAMP_SECONDS(ABS(device.time_zone_offset_seconds))) AS STRING),5) 
            AS device_time_zone_offset
        , STRUCT<name STRING, medium STRING, source STRING>(
            traffic_source.name, traffic_source.medium, traffic_source.source
        ) AS traffic_source
        , STRUCT<revenue FLOAT64, currency STRING>(
            user_ltv.revenue, user_ltv.currency
        ) AS users_ltv
        , STRUCT<firebase_app_id STRING, stream_id STRING, advertising_id STRING>(
            LOWER(app_info.firebase_app_id), LOWER(stream_id), LOWER({{ null_if_length_zero('device.advertising_id') }})
        ) as other_ids
        , {{ overbase_firebase.generate_date_timezone_struct('TIMESTAMP_MICROS(event_timestamp)') }} as event_dates
        , {{ overbase_firebase.generate_date_timezone_struct('TIMESTAMP_MICROS(user_first_touch_timestamp)') }} as install_dates
        , COUNT(1) OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) as duplicates_cnt
FROM 
(
{%- set projects = var('OVERBASE:SOURCES', []) -%}
{%- set ready = var('OVERBASE:SOURCES_READY', false) -%}

 {%- set first = (projects[0] if projects and (projects[0] is mapping) else {}) -%}
  {%- set pid0 = first.get('project_id', 'fallback_project') -%}
  {%- set ads_raw0 = first.get('analytics_dataset_ids') if first.get('analytics_dataset_ids') is not none else first.get('analytics_dataset_id') -%}
  {%- if ads_raw0 is string -%}
    {%- set ds0 = ads_raw0 -%}
  {%- elif ads_raw0 is iterable and (ads_raw0 | length) > 0 -%}
    {%- set ds0 = ads_raw0[0] -%}
  {%- else -%}
    {%- set ds0 = 'fallback_dataset' -%}
  {%- endif -%}

 {%- if not ready -%}
    -- FALLBACK: use the single parse-safe source until generated sources are ready
    SELECT
      '{{ pid0 }}' as project_id,
      '{{ ds0 }}'  as dataset_id,
      *
    FROM {{ source('firebase_analytics__fallback', 'events') }}
    WHERE {{ overbase_firebase.analyticsTableSuffixFilter() }}
 {%- else -%}
{%- set ns = namespace(first=true) -%}
{%- for p in projects -%}
  {%- set pid = p.get('project_id') -%}
  {%- if not pid %}{% continue %}{% endif -%}
  {%- set ads_raw = p.get('analytics_dataset_ids') if p.get('analytics_dataset_ids') is not none else p.get('analytics_dataset_id') -%}
  {%- if ads_raw is string -%}
    {%- set ads_list = [ads_raw] -%}
  {%- elif ads_raw is iterable -%}
    {%- set ads_list = ads_raw -%}
  {%- else -%}
    {%- set ads_list = [] -%}
  {%- endif -%}

  {%- for ds in ads_list -%}
    {%- if pid and ds -%}
      {% if not ns.first %}UNION ALL{% endif %}
      {% set ns.first = false %}
      SELECT
        '{{ pid }}' as project_id,
        '{{ ds }}' as dataset_id,
        *
      FROM {{ source('firebase_analytics__' ~ pid ~ '__' ~ ds, 'events') }}
      WHERE {{ overbase_firebase.analyticsTableSuffixFilter() }}
    {%- endif -%}
  {%- endfor -%}
{%- endfor -%}
{%- endif -%}
) as events
LEFT JOIN {{ref('ob_iso_country')}} as country_codes
    ON LOWER(events.geo.country) = LOWER(country_codes.firebase_name)
LEFT JOIN {{ref("ob_iso_language")}} as language_codes
    ON LOWER(SPLIT(events.device.language,'-')[SAFE_OFFSET(0)]) = language_codes.alpha_2
LEFT JOIN {{ref('ob_iso_country')}} as language_region_codes -- some language have 3 parts (e.g. zh-hans-us), so just get the last one
    ON LOWER(ARRAY_REVERSE(SPLIT(events.device.language,'-'))[SAFE_OFFSET(0)]) = language_region_codes.alpha_2


QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) = 1