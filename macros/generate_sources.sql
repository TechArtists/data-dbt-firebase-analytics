{% macro generate_firebase_sources(
    var_name='TA:SOURCES',
    source_prefix='firebase',
    indent=2,
    include_comments=true
) %}
  {# 
    var_name: name of the dbt var containing the projects list
    source_prefix: prefix for source names ("firebase")
    indent: number of spaces for indentation (2)
    include_comments: whether to include comment lines in YAML (true/false)
  #}

  {% set projects = var(var_name, []) %}

  {% if projects is not iterable or (projects | length == 0) %}
    {{ exceptions.raise_compiler_error(
      "Var '" ~ var_name ~ "' must be a non-empty list of dicts."
    ) }}
  {% endif %}

  {% set sp = ' ' * indent %}
  {% set nl = '\n' %}
  {% set out = [] %}

  {% do out.append('version: 2') %}
  {% do out.append('') %}
  {% do out.append('sources:') %}

  {% for p in projects %}
    {% set pid = (p.get('project_id') or '') | trim %}
    {% set ads = (p.get('analytics_dataset_id') or '') | trim %}
    {% set events_table = (p.get('events_table') or 'events_*') | trim %}
    {% set cds = (p.get('crashlytics_dataset_id') or '') | trim %}
    {% set crash_table = (p.get('crashlytics_table') or '') | trim %}

    {% if pid == '' or ads == '' %}
      {{ exceptions.raise_compiler_error("Missing required keys in " ~ var_name) }}
    {% endif %}

    {% if include_comments %}
      {% do out.append(sp ~ "# Analytics for " ~ pid) %}
    {% endif %}
    {% do out.append(sp ~ "- name: " ~ source_prefix ~ "_analytics__" ~ pid) %}
    {% do out.append(sp ~ "  database: " ~ pid) %}
    {% do out.append(sp ~ "  schema: " ~ ads) %}
    {% do out.append(sp ~ "  tables:") %}
    {% do out.append(sp ~ "    - name: events") %}
    {% do out.append(sp ~ "      identifier: " ~ events_table) %}
    {% do out.append('') %}

    {% if cds != '' %}
      {% if include_comments %}
        {% do out.append(sp ~ "# Crashlytics for " ~ pid) %}
      {% endif %}
      {% do out.append(sp ~ "- name: " ~ source_prefix ~ "_crashlytics__" ~ pid) %}
      {% do out.append(sp ~ "  database: " ~ pid) %}
      {% do out.append(sp ~ "  schema: " ~ cds) %}
      {% do out.append(sp ~ "  tables:") %}
      {% do out.append(sp ~ "    - name: events") %}
      {% do out.append(sp ~ "      identifier: " ~ crash_table) %}
      {% do out.append('') %}
    {% endif %}
  {% endfor %}

  {% set result = out | join(nl) | trim %}
  {{ print(result) }} 
  {{ return(result) }}
{% endmacro %}
