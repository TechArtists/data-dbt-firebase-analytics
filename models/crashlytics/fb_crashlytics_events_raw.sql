{{ ta_firebase.verify_all_ta_mandatory_variables() }}

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

-- https://firebase.google.com/docs/crashlytics/bigquery-export#without_stack_traces

{% set projects = var('TA:SOURCES', []) %}
{% set ready = var('TA:SOURCES_READY', false) %}

{% set first = projects[0] if projects and (projects[0] is mapping) else {} %}
{% set pid0 = first.get('project_id', 'fallback_project') %}
{% set ds0  = first.get('crashlytics_dataset_id', 'fallback_crashlytics_dataset') %}

{%- set ns = namespace(first=true) -%}

(
{% if not ready %}
    SELECT
    '{{ pid0 }}' AS project_id
    ,'{{ ds0 }}'  AS dataset_id
    , event_timestamp as event_ts
    , DATE(event_timestamp) as event_date
    , received_timestamp as received_ts
    , installation_uuid as crashlytics_user_pseudo_id
    , (SELECT value FROM UNNEST(custom_keys) WHERE key = 'fb_user_pseudo_id') as firebase_analytics_user_pseudo_id
    , COALESCE(user.id, (SELECT value FROM UNNEST(custom_keys) WHERE key = 'app_user_id')) as user_id
    , bundle_identifier as app_id
    , ARRAY_TO_STRING(ARRAY_REVERSE(SPLIT(bundle_identifier, '.')), '.') as reverse_app_id
    , event_id
         -- the platform we get in operating_system.type is not populated for Android, only for iOS. So rely on _TABLE_SUFFIX instead
    , CASE WHEN _TABLE_SUFFIX LIKE '%ANDROID%' THEN'ANDROID'
               WHEN _TABLE_SUFFIX LIKE '%IOS%' THEN'IOS'
               ELSE 'UNKNOWN' -- TODO: unit test for this
          END as platform 
    , STRUCT<id STRING, title STRING, subtitle STRING, variant_id STRING>(
        issue_id, issue_title, issue_subtitle, variant_id
    ) as issue
    , error_type
        , process_state
        , STRUCT<app STRING, device STRING>(
        	app_orientation, device_orientation
          ) as orientation
        , STRUCT<firebase_value STRING, build_no STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "application.display_version" -%}
            {{ v }}, application.build_version, {{ ta_firebase.get_version(v, "major") }}, {{ ta_firebase.get_version(v, "minor") }}, {{ ta_firebase.get_version(v, "bugfix") }}, {{ ta_firebase.get_version(v, "major.minor") }}, {{ ta_firebase.get_version(v, "major.minor.bugfix") }}, {{ ta_firebase.get_version(v, "normalized") }}, COALESCE(CAST({{ ta_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS app_version
        , STRUCT<firebase_value STRING, name STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "operating_system.display_version" -%}
            {{ v }}, operating_system.name, {{ ta_firebase.get_version(v, "major") }}, {{ ta_firebase.get_version(v, "minor") }}, {{ ta_firebase.get_version(v, "bugfix") }}, {{ ta_firebase.get_version(v, "major.minor") }}, {{ ta_firebase.get_version(v, "major.minor.bugfix") }}, {{ ta_firebase.get_version(v, "normalized") }}, COALESCE(CAST( {{ ta_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS platform_version
        , operating_system.modification_state as jailbroken_state
        , STRUCT<type STRING, manufacturer STRING, os_model STRING, architecture STRING>(
            LOWER(operating_system.device_type), LOWER(device.manufacturer), LOWER(device.model), device.architecture 
        ) AS device_hardware
        , {{ ta_firebase.generate_struct_for_raw_crashlytics_custom_keys() }} as custom_keys
        , custom_keys as custom_keys_raw
        , STRUCT<used_bytes INT64, free_bytes INT64>(memory.used, memory.free) as memory
        , STRUCT<used_bytes INT64, free_bytes INT64>(storage.used, storage.free) as storage
        , STRUCT<name STRING, email STRING>(user.name, user.email) as user
        , crashlytics_sdk_version AS crashlytics_sdk_version_string
        , logs
        , breadcrumbs
        , blame_frame
        , exceptions as android_exceptions
        , errors as ios_non_fatal
        , threads
        , STRUCT<unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT64, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING>(
          {#  it has a short form of 20 columns (iOS REALTIME only) and a long form of 26 columns (Android historic, Android realtime & iOS historic )
          20:unity_version STRING,debug_build BOOLEAN,processor_type STRING,processor_count INTEGER,processor_frequency_mhz INTEGER,system_memory_size_mb INTEGER,graphics_memory_size_mb INTEGER,graphics_device_id INTEGER,graphics_device_vendor_id INTEGER,graphics_device_name STRING,graphics_device_vendor STRING,graphics_device_version STRING,graphics_device_type STRING,graphics_shader_level INTEGER,graphics_render_target_count INTEGER,graphics_copy_texture_support STRING,graphics_max_texture_size INTEGER,screen_size_px STRING,screen_refresh_rate_hz INTEGER,screen_resolution_dpi STRING,
          sometimes it's processor_frequency_mhz 
          26: unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING
          Differences:
             + processor_frequency (but both also have processor_frequency_mhz)
             + system_memory_size (but both also have system_memory_size_mb)
             + graphics_memory_size (but both also have graphics_memory_size_mb)
             + screen_size
             + screen_dpi
             + screen_refresh_rate
            Those values are NULLed for the time being
          #}
          {{ ta_firebase.list_map_and_add_prefix([
            "unity_version","debug_build","processor_type","processor_count",none,none,none,"graphics_device_id","graphics_device_vendor_id","graphics_device_name","graphics_device_vendor","graphics_device_version","graphics_device_type","graphics_shader_level","graphics_render_target_count","graphics_copy_texture_support","graphics_max_texture_size",none,none,none,"processor_frequency_mhz","system_memory_size_mb","graphics_memory_size_mb","screen_size_px","screen_refresh_rate_hz","screen_resolution_dpi"
            ], "unity_metadata." )| join(", ") }}
          ) AS unity_metadata
        , COUNT(1) OVER (PARTITION BY installation_uuid, event_id, variant_id) as duplicates_cnt

    FROM {{ source('firebase_crashlytics__fallback', 'events') }}
    WHERE {{ ta_firebase.crashlyticsTSFilterFor("event_timestamp") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY crashlytics_user_pseudo_id, event_id, variant_id ORDER BY received_ts) = 1

{% else %}

    {% for p in projects %}
        {% set pid = p.get('project_id') %}
        {% set ds  = p.get('crashlytics_dataset_id') %}

        {% if pid and ds %}
            {% if not ns.first %}UNION ALL{% endif %}
            {% set ns.first = false %}

            SELECT
                '{{ pid }}' AS project_id,
                '{{ ds }}'  AS dataset_id,
                 DATE(event_timestamp) as event_date
    , received_timestamp as received_ts
    , installation_uuid as crashlytics_user_pseudo_id
    , (SELECT value FROM UNNEST(custom_keys) WHERE key = 'fb_user_pseudo_id') as firebase_analytics_user_pseudo_id
    , COALESCE(user.id, (SELECT value FROM UNNEST(custom_keys) WHERE key = 'app_user_id')) as user_id
    , bundle_identifier as app_id
    , ARRAY_TO_STRING(ARRAY_REVERSE(SPLIT(bundle_identifier, '.')), '.') as reverse_app_id
    , event_id
         -- the platform we get in operating_system.type is not populated for Android, only for iOS. So rely on _TABLE_SUFFIX instead
    , CASE WHEN _TABLE_SUFFIX LIKE '%ANDROID%' THEN'ANDROID'
               WHEN _TABLE_SUFFIX LIKE '%IOS%' THEN'IOS'
               ELSE 'UNKNOWN' -- TODO: unit test for this
          END as platform 
    , STRUCT<id STRING, title STRING, subtitle STRING, variant_id STRING>(
        issue_id, issue_title, issue_subtitle, variant_id
    ) as issue
    , error_type
        , process_state
        , STRUCT<app STRING, device STRING>(
        	app_orientation, device_orientation
          ) as orientation
        , STRUCT<firebase_value STRING, build_no STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "application.display_version" -%}
            {{ v }}, application.build_version, {{ ta_firebase.get_version(v, "major") }}, {{ ta_firebase.get_version(v, "minor") }}, {{ ta_firebase.get_version(v, "bugfix") }}, {{ ta_firebase.get_version(v, "major.minor") }}, {{ ta_firebase.get_version(v, "major.minor.bugfix") }}, {{ ta_firebase.get_version(v, "normalized") }}, COALESCE(CAST({{ ta_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS app_version
        , STRUCT<firebase_value STRING, name STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "operating_system.display_version" -%}
            {{ v }}, operating_system.name, {{ ta_firebase.get_version(v, "major") }}, {{ ta_firebase.get_version(v, "minor") }}, {{ ta_firebase.get_version(v, "bugfix") }}, {{ ta_firebase.get_version(v, "major.minor") }}, {{ ta_firebase.get_version(v, "major.minor.bugfix") }}, {{ ta_firebase.get_version(v, "normalized") }}, COALESCE(CAST( {{ ta_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS platform_version
        , operating_system.modification_state as jailbroken_state
        , STRUCT<type STRING, manufacturer STRING, os_model STRING, architecture STRING>(
            LOWER(operating_system.device_type), LOWER(device.manufacturer), LOWER(device.model), device.architecture 
        ) AS device_hardware
        , {{ ta_firebase.generate_struct_for_raw_crashlytics_custom_keys() }} as custom_keys
        , custom_keys as custom_keys_raw
        , STRUCT<used_bytes INT64, free_bytes INT64>(memory.used, memory.free) as memory
        , STRUCT<used_bytes INT64, free_bytes INT64>(storage.used, storage.free) as storage
        , STRUCT<name STRING, email STRING>(user.name, user.email) as user
        , crashlytics_sdk_version AS crashlytics_sdk_version_string
        , logs
        , breadcrumbs
        , blame_frame
        , exceptions as android_exceptions
        , errors as ios_non_fatal
        , threads
        , STRUCT<unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT64, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING>(
          {#  it has a short form of 20 columns (iOS REALTIME only) and a long form of 26 columns (Android historic, Android realtime & iOS historic )
          20:unity_version STRING,debug_build BOOLEAN,processor_type STRING,processor_count INTEGER,processor_frequency_mhz INTEGER,system_memory_size_mb INTEGER,graphics_memory_size_mb INTEGER,graphics_device_id INTEGER,graphics_device_vendor_id INTEGER,graphics_device_name STRING,graphics_device_vendor STRING,graphics_device_version STRING,graphics_device_type STRING,graphics_shader_level INTEGER,graphics_render_target_count INTEGER,graphics_copy_texture_support STRING,graphics_max_texture_size INTEGER,screen_size_px STRING,screen_refresh_rate_hz INTEGER,screen_resolution_dpi STRING,
          sometimes it's processor_frequency_mhz 
          26: unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING
          Differences:
             + processor_frequency (but both also have processor_frequency_mhz)
             + system_memory_size (but both also have system_memory_size_mb)
             + graphics_memory_size (but both also have graphics_memory_size_mb)
             + screen_size
             + screen_dpi
             + screen_refresh_rate
            Those values are NULLed for the time being
          #}
          {{ ta_firebase.list_map_and_add_prefix([
            "unity_version","debug_build","processor_type","processor_count",none,none,none,"graphics_device_id","graphics_device_vendor_id","graphics_device_name","graphics_device_vendor","graphics_device_version","graphics_device_type","graphics_shader_level","graphics_render_target_count","graphics_copy_texture_support","graphics_max_texture_size",none,none,none,"processor_frequency_mhz","system_memory_size_mb","graphics_memory_size_mb","screen_size_px","screen_refresh_rate_hz","screen_resolution_dpi"
            ], "unity_metadata." )| join(", ") }}
          ) AS unity_metadata
        , COUNT(1) OVER (PARTITION BY installation_uuid, event_id, variant_id) as duplicates_cnt

            FROM {{ source('firebase_crashlytics__' ~ pid, 'events') }}
            WHERE {{ ta_firebase.crashlyticsTSFilterFor("event_timestamp") }}

        {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY crashlytics_user_pseudo_id, event_id, variant_id ORDER BY received_ts) = 1
    {% endfor %}
{% endif %}
)


