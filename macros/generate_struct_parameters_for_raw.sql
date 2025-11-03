{% macro generate_struct_for_raw_user_properties() -%}
    {{ overbase_firebase.generate_struct(overbase_firebase.get_user_property_tuples_all(), 'user_properties') }}
{%- endmacro%}

{% macro generate_struct_for_raw_event_parameters() -%}
    {{ overbase_firebase.generate_struct(overbase_firebase.get_event_parameter_tuples_for_raw(), 'event_params') }}
{%- endmacro%}

{% macro generate_struct_for_raw_crashlytics_custom_keys() -%}
    {{ overbase_firebase.generate_struct(overbase_firebase.get_crashlytics_custom_key_tuples_all(), 'custom_keys') }}
{%- endmacro%}


{% macro generate_struct(all_parameters, firebase_record_name) -%}
    {{ return(adapter.dispatch('generate_struct', 'overbase_firebase')(all_parameters, firebase_record_name) ) }}
{%- endmacro %}


{# STRUCT<ob_ui_dark_mode_string STRING, plays_progressive_string STRING, first_open_time_int INT64, poorly_set_variable_double DOUBLE>(
     (SELECT LOWER(value.string_value) from UNNEST(user_properties) WHERE key = 'ob_ui_dark_mode') , (SELECT LOWER(value.string_value) from UNNEST(user_properties) WHERE key = 'plays_progressive') , (SELECT value.int_value from UNNEST(user_properties) WHERE key = 'first_open_time') , (SELECT value.double_value from UNNEST(user_properties) WHERE key = 'poorly_set_variable') 
 )  #}
{% macro bigquery__generate_struct(all_parameters, firebase_record_name) -%}

    {%- set structFieldNames = [] -%}
    {%- for parameter in all_parameters -%}
        {%- set key_name = parameter['key_name'] -%}
        {%- set struct_field_name = parameter['struct_field_name'] -%}
        {%- set bq_type = parameter['output_data_type'] -%}
        {%- set _ = structFieldNames.append( struct_field_name ~ " " ~ bq_type ) -%}
    {%- endfor -%}

    {%- set structValues = [] -%}
    {% for parameter in all_parameters -%}
        {%- set key_name = parameter['key_name'] -%}
        {%- set extract_transformation = parameter['extract_transformation'] -%}
        {%- set event_name_condition = overbase_firebase.makeListIntoSQLInFilter("event_name", parameter['event_name_filter']) -%}
        {%- set _ = structValues.append("(SELECT IF(" ~ event_name_condition ~ "," ~ extract_transformation ~ ",NULL) FROM  UNNEST(" ~ firebase_record_name ~ ") WHERE key = '" ~ key_name + "')") -%}
    {%- endfor -%}
    {%- if structFieldNames|length > 0 -%}
      STRUCT<{{ structFieldNames | join(",")}}>(
          {{ structValues | join(",") }}
      )
    {%- else -%}
        STRUCT<empty STRING>('noCustomKeysDefined')
    {%- endif -%}
{%- endmacro %}

