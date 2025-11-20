{% macro generate_sources_multiple_projects(
    var_name='TA:SOURCES',
    source_prefix='firebase',
    indent=2,
    include_comments=true
) %}
  {% set sp = ' ' * indent %}
  {% set nl = '\n' %}
  {% set out = [] %}

  {% set projects = var(var_name, []) %}
  {% if projects is string %}{% set projects = fromjson(projects) %}{% endif %}

  {% do out.append('version: 2') %}
  {% do out.append('') %}
  {% do out.append('sources:') %}

  {% for p in projects %}
    {% set pid = (p.get('project_id') or '') | trim %}
    {% set events_table = (p.get('events_table') or 'events_*') | trim %}
    {% set intraday_table = (p.get('intraday_table') or 'events_intraday_*') | trim %}
    {% set cds = (p.get('crashlytics_dataset_id') or '') | trim %}
    {% set crash_table = (p.get('crashlytics_table') or 'com_labpixies_flood_floodit_*') | trim %}

    {# accept either analytics_dataset_ids (list) or analytics_dataset_id (string) #}
    {% set ads_raw = p.get('analytics_dataset_ids') if p.get('analytics_dataset_ids') is not none else p.get('analytics_dataset_id') %}
    {% if ads_raw is string %}
      {% set ads_list = [ads_raw | trim] %}
    {% elif ads_raw is iterable %}
      {% set ads_list = ads_raw %}
    {% else %}
      {% set ads_list = [] %}
    {% endif %}

    {% for ads in ads_list %}
      {% set ds = ads | trim %}
      {% if ds != '' %}
        {% if include_comments %}
          {% do out.append(sp ~ "# Analytics for " ~ pid ~ " (" ~ ds ~ ")") %}
        {% endif %}
        {% do out.append(sp ~ "- name: " ~ source_prefix ~ "_analytics__" ~ pid ~ "__" ~ ds) %}
        {% do out.append(sp ~ "  database: " ~ pid) %}
        {% do out.append(sp ~ "  schema: " ~ ds) %}
        {% do out.append(sp ~ "  tables:") %}
        {% do out.append(sp ~ "    - name: events") %}
        {% do out.append(sp ~ "      identifier: " ~ events_table) %}
        {% do out.append(sp ~ "    - name: events_intraday") %}
        {% do out.append(sp ~ "      identifier: " ~ intraday_table) %}
        {% do out.append('') %}
      {% endif %}
    {% endfor %}

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