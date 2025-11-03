{%- set columnNamesEventDimensions = ["event_dates", "install_dates", "app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}


{%- set miniColumnsToIgnoreInGroupBy = overbase_firebase.get_mini_columns_to_ignore_when_rolling_up() -%}

{# Ignore all time zones except the first & last (they're all the same, just save the computational effort) #}
{%- set timezones = overbase_firebase.generate_date_timezone_age_struct('dont care') | map(attribute=0) | list -%}
{%- set timezones = timezones[1:-1] -%}
{%- set miniColumnsToIgnoreInGroupBy = miniColumnsToIgnoreInGroupBy + overbase_firebase.list_map_and_add_prefix(timezones, 'event_dates.') + overbase_firebase.list_map_and_add_prefix(timezones, 'install_dates.') -%}


{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}


{%- set minicolumns = overbase_firebase.unpack_columns_into_minicolumns_array(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") -%}
{%- set unionAllSelects = [] -%}

WITH
{%- for column in minicolumns -%}
    {{ ", " if not loop.first else "" }} dim_{{loop.index}} AS ( SELECT COUNT(DISTINCT({{ column[0] }})) AS dist_cnt FROM  {{ ref("fb_analytics_events_raw") }} WHERE DATE(event_date) = '2023-10-10')
    {% set _ = unionAllSelects.append("SELECT '" ~ column[1]  ~ "' AS dim_name, dist_cnt FROM dim_" ~ loop.index) -%}
{% endfor -%}
 
{{ unionAllSelects | join('\n UNION ALL ') }}

ORDER BY 2 DESC