{# Returns the Database Columns & the count of the unnested ones.
    A column returned by adapter.get_columns_in_relation has multiple properties, for example:
        column["name"]: app_id     , column["data_type"] STRING
        column["name"]: app_version, column["data_type"] STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>

    This macro will return those 2 columns & the overall count, including the unnested mini columns
        2 columns: app_id STRING, app_version STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>
        contOfUnnested: 6
#}
{% macro get_filtered_columns_for_table(table_name, columnsToFind, miniColumnsToIgnore) -%}

    {%- set columnsToFindSet = set(columnsToFind) -%}
    {%- set miniColumnsToIgnoreSet = set(miniColumnsToIgnore) -%}
    {%- set columnsUnnestedCount = [] -%}

    {%- set columns = ta_firebase.convert_columns_to_dicts(adapter.get_columns_in_relation(ref(table_name))) -%}
    {%- set columnsToGroupBy = [] -%}

    {%- for column in columns -%}
        {%- set columnName = column.name | replace('`', '') -%}
        {%- if columnsToFind == "*" or columnName in columnsToFindSet -%}
            {%- if not column["data_type"].startswith('STRUCT') -%}
                {%- if columnName not in miniColumnsToIgnoreSet -%}
                    {{ columnsToGroupBy.append(column) or "" }}
                    {% set _ = columnsUnnestedCount.append(1) %}
                {%- endif -%}
            {%- else %}
                {# remove the 'STRUCT<' prefix, then split by ' ' and get every other item, ie the mini column name  #}
                {%- for structMiniColumnTmp in column["data_type"][7:-1].split(' ')[::2] -%}
                    {%- set structMiniColumn = structMiniColumnTmp | replace('`', '') -%}
                    {%- if columnName ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet %}
                        {% set _ = columnsUnnestedCount.append(1) %}
                    {%- endif -%}
                {%- endfor -%}
                {{ columnsToGroupBy.append(column) or "" }}
            {%- endif -%}
        {%- endif -%}
    {%- endfor %} 

    {{ return([columnsToGroupBy, columnsUnnestedCount | sum() ]) }}

{%- endmacro -%}

{%- macro convert_columns_to_dicts(columns) -%}
{%- set res = [] -%}
{%- for column in columns -%}
    {# for STRUCTs, BQ will write backticks too: STRUCT<`ta_view_name_string` STRING #}
    {%- set _ = res.append({"data_type": column.data_type.replace('`', ''), "name": column.name }) -%}
{%- endfor -%}
{{ return(res) }}
{%- endmacro -%}