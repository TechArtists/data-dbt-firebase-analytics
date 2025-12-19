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


{%- set columnNamesEventDimensions = ["bundle_id", "reverse_bundle_id", "issue", "platform", "error_type", "process_state"
                                     ,"orientation", "app_version", "platform_version", "jailbroken_state"
                                     ,"device_hardware", "custom_keys"
] -%}

{%- set miniColumnsToIgnoreInGroupBy = [] -%}
{%- set tmp_res = ta_firebase.get_filtered_columns_for_table("crashlytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}
 
WITH data as (
    SELECT   DATE(event_ts) as event_date
            , project_id
            , dataset_id
            , {{ ta_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}
            , COUNT(1) as cnt -- same as COUNT(DISTINCT(event_id))
            , COUNT(DISTINCT(crashlytics_user_pseudo_id)) as users
    FROM {{ ref("crashlytics_events_raw") }}
    WHERE {{ ta_firebase.analyticsDateFilterFor('event_date') }}
    GROUP BY 1,2,3 {% for n in range(4, 4 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT event_date
        , {{ ta_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
        , cnt
        , users
FROM data