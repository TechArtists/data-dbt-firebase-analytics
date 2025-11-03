{# One-off macro to be used as an operation that can update the BigQuery column descriptions, without needing to delete & re-create the table

Run as: dbt run-operation update_bigquery_doc --args 'model_name: fb_analytics_events_raw'

Taken from: https://github.com/dbt-labs/dbt-core/issues/4226 
#}
{% macro update_bigquery_doc(model_name, relation = true, columns = true) %}

  {% if execute %}
    {% set model_node = (graph.nodes.values() | selectattr('name', 'equalto', model_name) | list)[0] %}
    {% set relation = adapter.get_relation(
        database=model_node.database, schema=model_node.schema, identifier=model_node.alias
    ) %}
  
    {% if 'description' in model_node %}
        {{ log("Altering table description for " + model_name, info = true) }}
        {{ adapter.update_table_description(model_node['database'], model_node['schema'], model_node['alias'], model_node['description']) }}
    {% endif %}
    {{ log("Altering column comments for " + model_name, info = true) }}
    {{ alter_column_comment(relation, model_node.columns) }}
  
  {% endif %}

{% endmacro %}
