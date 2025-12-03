{{ config(
    materialized='ephemeral',
    enabled=false
) }}

SELECT 1 as dont_care
{# FROM {{ ref("google_analytics_events") }} #}
{# FROM {{ ref("google_analytics_installs") }} #}