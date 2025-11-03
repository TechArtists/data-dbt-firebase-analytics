{% macro get_version(version, type) -%}
    {{ return(adapter.dispatch('get_version', 'overbase_firebase')(version, type)) }}
{%- endmacro %}

{% macro bigquery__get_version(version, type) -%}
    {%- set cleaned_version = "REGEXP_EXTRACT(" ~ version ~ ", r'^(\\d+(?:\\.\\d+){0,2})')" -%}
    {%- if type == 'major' -%}
        SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(0)] AS INT64)
    {%- elif type == 'minor' -%}
        SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(1)] AS INT64)
    {%- elif type == 'bugfix' -%}
        SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(2)] AS INT64)
    {%- elif type == 'major.minor' -%}
        SAFE_CAST(CONCAT( {{ overbase_firebase.get_version(cleaned_version, 'major') }} , ".",  COALESCE({{ overbase_firebase.get_version(cleaned_version, 'minor') }}, 0) ) AS FLOAT64)
    {%- elif type == 'major.minor.bugfix' -%}
        {#  17 -> 17; 17.0 -> 17.0; 17.0.1 -> 17.0.1  #}
        CASE WHEN REGEXP_CONTAINS({{ cleaned_version }}, r'^[0-9.]*$') AND ARRAY_LENGTH(SPLIT({{ cleaned_version }}, '.')) <= 3 THEN
                SAFE_CAST (
                    CONCAT(         SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(0)]
                         , COALESCE(CONCAT('.', SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(1)]),'')
                         , COALESCE(CONCAT('.', SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(2)]),'')
                    ) AS STRING)
            ELSE NULL
        END
    {%- elif type == 'normalized' -%}
        CASE WHEN REGEXP_CONTAINS({{ cleaned_version }}, r'^[0-9.]*$') AND ARRAY_LENGTH(SPLIT({{ cleaned_version }}, '.')) <= 3 THEN
                SAFE_CAST (
                    CONCAT(COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(0)] AS INT64)),'000000')
                         , COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(1)] AS INT64)),'000000')
                         , COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ cleaned_version }}, '.')[SAFE_OFFSET(2)] AS INT64)),'000000')
                    ) AS INT64)
            ELSE NULL
        END
    {%- else -%}
       {{ exceptions.raise_compiler_error("Unknown type '%s'" % type) }}
    {%- endif -%}
{%- endmacro %}

{# Returns major, minor, bugfix, major.minor, majoir.minor.bugfix, normalized #}
{%- macro get_version_record_from_normalized(normalizedAsString) -%}
  CASE WHEN SAFE_CAST({{ normalizedAsString }} AS INT64) IS NOT NULL AND LENGTH({{ normalizedAsString }}) >= 13
       THEN STRUCT<major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64>(
          SAFE_CAST(SUBSTR({{ normalizedAsString }}, 0, LENGTH({{ normalizedAsString }}) - 12) AS INT64), 
          SAFE_CAST(SUBSTR({{ normalizedAsString }}, -12, 6) AS INT64),
          SAFE_CAST(SUBSTR({{ normalizedAsString }}, -6, 6) AS INT64),
          SAFE_CAST(CONCAT(SAFE_CAST(SUBSTR({{ normalizedAsString }}, 0, LENGTH({{ normalizedAsString }}) - 12) AS INT64), ".", SAFE_CAST(SUBSTR({{ normalizedAsString }}, -12, 6) AS INT64)) AS FLOAT64),
          CONCAT(SAFE_CAST(SUBSTR({{ normalizedAsString }}, 0, LENGTH({{ normalizedAsString }}) - 12) AS INT64), ".", SAFE_CAST(SUBSTR({{ normalizedAsString }}, -12, 6) AS INT64), ".", SAFE_CAST(SUBSTR({{ normalizedAsString }}, -6, 6) AS INT64)),
          SAFE_CAST({{ normalizedAsString }} AS INT64)
   )
  ELSE STRUCT<major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64>(
          NULL, NULL, NULL, NULL, NULL, NULL
  )
  END

{%- endmacro -%}
