-- a variable that must be overwritten when ta Analytics via FB is used in another DBT project
-- the default value is meant only to be used within this project, as test values 
{% macro compile_time_mandatory_var(variable_name, default_value_in_ta) -%}
{% if var(variable_name, default_value_in_ta) == default_value_in_ta and env_var('DBT_INSIDE_TA','')|length == 0 -%}
   {{ exceptions.raise_compiler_error("Variable '%s' must be overwritten inside your dbt_project.yml. Check the TA documentation to see all mandatory variables" % variable_name) }}
{% else %}
{%- endif %}
{%- endmacro %}

{% macro verify_all_ta_mandatory_variables() -%}
{{- ta_firebase.compile_time_mandatory_var("TA:SOURCES", "ta") -}}
{{- ta_firebase.compile_time_mandatory_var("TA:SOURCES_READY", "firebase_analytics_raw_test") -}}
{{- ta_firebase.compile_time_mandatory_var("TA:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE", "2018-01-01") -}}
{{- ta_firebase.compile_time_mandatory_var("TA:FIREBASE_CRASHLYTICS_FULL_REFRESH_START_DATE", "2018-01-01") -}}



{%- endmacro %}