{{ config(
    materialized='ephemeral',
    enabled = false
) }}

SELECT 1 as dont_care
{# FROM {{ ref("google_analytics_events_wid") }} #}
{# FROM {{ ref("google_analytics_installs") }} #}