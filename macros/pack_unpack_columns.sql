{# Returns an array of tuples of (column, alias). If it's a struct, it extracts the value from within it. Example: 
     [  ("$tablePrefix.app_id", "$aliasPrefix.app_id"),
        ("$tablePrefix.event_name", "$aliasPrefix.event_name"),
        ("$tablePrefix.app_version.firebase_value", "$aliasPrefix.app_version_firebase_value"),
        ("$tablePrefix.app_version.major", "$aliasPrefix.app_version_major")
        ("$tablePrefix.app_version.minor", "$aliasPrefix.app_version_minor")]
#}
{%- macro unpack_columns_into_minicolumns_array(columns, miniColumnsToIgnore, miniColumnsToNil, tablePrefix, aliasPrefix) -%}
    {%- set result = [] -%}
    {%- set miniColumnsToIgnoreSet = set(miniColumnsToIgnore) -%}
    {%- set miniColumnsToNilSet = set(miniColumnsToNil) -%}
    {%- for column in columns -%}
        {%- set columnName = column["name"] | replace('`', '') -%}

        {%- if not column["data_type"].startswith('STRUCT') -%}
            {%- set _ = result.append( (tablePrefix ~ columnName, aliasPrefix ~ columnName) ) -%}
        {%- else -%}
            {# remove the 'STRUCT<' prefix, then split by ' ' and get every other item, ie the mini column name  #}
            {# STRUCT<ob_view_name_string STRING, ob_view_type_string STRING -> ["ob_view_name_string", "STRING", "ob_view_type_string", "STRING"] #}
            {%- for structMiniColumnTmp in column["data_type"][7:-1].split(' ')[::2] -%}
                {%- set structMiniColumn = structMiniColumnTmp | replace('`', '') -%}
                {%- if columnName ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet %}
                    {%- if columnName ~ "." ~ structMiniColumn in miniColumnsToNil %}
                        {%- set _ = result.append( ("'ob-forced-null'", aliasPrefix ~ columnName ~ "_" ~ structMiniColumn) ) -%}
                    {%- else -%}
                        {%- set _ = result.append( (tablePrefix ~ columnName ~ "." ~ structMiniColumn, aliasPrefix ~ columnName ~ "_" ~ structMiniColumn) ) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(result) }}
{%- endmacro -%}


{# Returns just the column name if it's not a struct, otherwise it unnests it. Example: 
     app_id, event_name, platform, appstore
    , app_version.firebase_value as app_version_firebase_value
    , app_version.major as app_version_major
    , app_version.minor as app_version_minor
    , app_version.bugfix as app_version_bugfix
    , app_version.major_minor as app_version_major_minor
    , app_version.normalized as app_version_normalized
#}
{%- macro unpack_columns_into_minicolumns(columns, miniColumnsToIgnore, miniColumnsToNil, tablePrefix, aliasPrefix) -%}
    {%- set minicolumns = overbase_firebase.unpack_columns_into_minicolumns_array(columns, miniColumnsToIgnore, miniColumnsToNil, tablePrefix, aliasPrefix) -%}
    {%- for minicolumn in minicolumns -%}
                   {{ ", " if not loop.first else "" }} {{ minicolumn[0] ~ " AS " ~ minicolumn[1] }}
    {% endfor -%}
{%- endmacro -%}


{# Packs unpacked columns
app_id 
        , event_name 
        , platform 
        , appstore 
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64> (
            app_version_firebase_value , app_version_major , app_version_minor , app_version_bugfix , app_version_major_minor , app_version_normalized 
        ) as app_version
        #}
{%- macro pack_minicolumns_into_structs_for_select(columns, miniColumnsToIgnore, unpackedAliasPrefix, packedAliasPrefix) -%}
    {%- set miniColumnsToIgnoreSet = set(miniColumnsToIgnore) -%}
    {%- for column in columns -%}
        {%- set columnName = column["name"] | replace('`', '') -%}

        {%- if not column.data_type.startswith('STRUCT') %}
            {{ ", " if not loop.first else "" }}{{ unpackedAliasPrefix ~ columnName }} AS {{ packedAliasPrefix ~ columnName }}
        {%- else -%}
            {#- ['ob_view_name STRING',] -#}
            {%- set structDefinitionDDLs = [] %}
            {%- for structMiniColumnDDLTmp in column["data_type"][7:-1].split(',') -%}
                {%- set structMiniColumnDDL = structMiniColumnDDLTmp | replace('`', '') -%}
                {% if columnName ~ "." ~ structMiniColumnDDL.strip().split(' ')[0] not in miniColumnsToIgnoreSet -%}
                    {%- set _ = structDefinitionDDLs.append(structMiniColumnDDL) -%}
                {%- endif -%}
            {%- endfor %}
            {%- set structValues = [] %}
            {% for structMiniColumnTmp in column["data_type"][7:-1].split(' ')[::2] -%}
                {%- set structMiniColumn = structMiniColumnTmp | replace('`', '') -%}
                {% if columnName ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet -%}
                     {%- set _ = structValues.append(unpackedAliasPrefix ~ columnName ~ "_" ~ structMiniColumn) -%}
                {%- endif -%}
            {%- endfor -%}
            {{ ", " if not loop.first else "" }} STRUCT<{{ structDefinitionDDLs | join(", ") }}>(
                {{ structValues | join(", ") }} 
            ) as {{ packedAliasPrefix ~ columnName }}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}
