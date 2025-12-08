{{ config(severity='error') }}

WITH stg AS (
  SELECT
    event_date,
    project_id,
    SUM(duplicates_cnt) AS cnt
  FROM {{ ref('google_analytics_events_raw') }}
  WHERE {{ ta_firebase.analyticsTestDateFilter('event_date', extend=2) }}
    AND event_date <= CURRENT_DATE() - 5
  GROUP BY 1, 2
),

src AS (
  {%- set projects = var('TA:SOURCES', []) -%}
  {%- set ready    = var('TA:SOURCES_MULTIPLE_PROJECTS_GENERATED', false) -%}
  {%- set ns = namespace(first=true) -%}

  {%- if not ready -%}
    SELECT
      DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
      COUNT(*) AS cnt,
      {{ (projects[0]['project_id'] if projects and (projects[0] is mapping) and projects[0].get('project_id') else 'single_project') | tojson }} AS project_id
    FROM {{ source('firebase_analytics__single_project', 'events') }}
    WHERE {{ ta_firebase.analyticsTestTableSuffixFilter(extend=3) }}
      AND {{ ta_firebase.analyticsTestDateFilter('DATE(TIMESTAMP_MICROS(event_timestamp))', extend=2) }}
      AND DATE(TIMESTAMP_MICROS(event_timestamp)) <= CURRENT_DATE() - 5
    GROUP BY 1


  {%- else -%}
    {%- for p in projects -%}
      {%- set pid = p.get('project_id') -%}
      {%- if not pid %}{% continue %}{% endif -%}

      {# Support either a single dataset_id or a list analytics_dataset_ids #}
      {%- set ads_raw = p.get('analytics_dataset_ids')
                         if p.get('analytics_dataset_ids') is not none
                         else p.get('analytics_dataset_id') -%}
      {%- if ads_raw is string -%}
        {%- set ads_list = [ads_raw] -%}
      {%- elif ads_raw is iterable -%}
        {%- set ads_list = ads_raw -%}
      {%- else -%}
        {%- set ads_list = [] -%}
      {%- endif -%}

      {%- if ads_list | length == 0 -%}
        {# Naming style: firebase_analytics__<pid> #}
        {% if not ns.first %} UNION ALL {% endif %}
        {%- set ns.first = false -%}
        SELECT
          DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
          COUNT(*) AS cnt,
          '{{ pid }}' AS project_id
        FROM {{ source('firebase_analytics__' ~ pid, 'events') }}
        WHERE {{ ta_firebase.analyticsTestTableSuffixFilter(extend=3) }}
          AND {{ ta_firebase.analyticsTestDateFilter('DATE(TIMESTAMP_MICROS(event_timestamp))', extend=2) }}
          AND DATE(TIMESTAMP_MICROS(event_timestamp)) <= CURRENT_DATE() - 5
        GROUP BY 1

      {%- else -%}
        {# Naming style: firebase_analytics__<pid>__<dataset> for each dataset #}
        {%- for ds in ads_list -%}
          {% if not ns.first %} UNION ALL {% endif %}
          {%- set ns.first = false -%}
          SELECT
            DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
            COUNT(*) AS cnt,
            '{{ pid }}' AS project_id
          FROM {{ source('firebase_analytics__' ~ pid ~ '__' ~ ds, 'events') }}
          WHERE {{ ta_firebase.analyticsTestTableSuffixFilter(extend=3) }}
            AND {{ ta_firebase.analyticsTestDateFilter('DATE(TIMESTAMP_MICROS(event_timestamp))', extend=2) }}
            AND DATE(TIMESTAMP_MICROS(event_timestamp)) <= CURRENT_DATE() - 5
          GROUP BY 1
        {%- endfor -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
)

SELECT *
FROM stg
LEFT JOIN src
  ON stg.event_date = src.event_date
 AND stg.project_id = src.project_id
WHERE stg.cnt <> src.cnt