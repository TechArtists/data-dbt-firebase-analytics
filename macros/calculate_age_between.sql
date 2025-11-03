{# TIMESTAMP_DIFF(.. HOUR), returns the floored hour. So something that is 1386 minutes passed (54 minutes till 24) will return as 23 hours #}
{# divide by 24.0 -> floor it -> cast to int #}

{# TIMESTAMP_DIFF('2023-10-13 19:18:56.955001', '2023-10-12 19:20:28.013000', MINUTE) -> 1438 #}
{# TIMESTAMP_DIFF('2023-10-13 19:18:56.955001', '2023-10-12 19:20:28.013000', HOUR)   -> 23 #}
{# DATETIME_DIFF ('2023-10-13 19:18:56.955001', '2023-10-12 19:20:28.013000', MINUTE) -> 1438 #}
{# DATETIME_DIFF ('2023-10-13 19:18:56.955001', '2023-10-12 19:20:28.013000', HOUR)   -> 24#}

{# Using "HOUR" works only timestamps correctly, but use minutes in both timestamp/datetime cases for consistency #}
{%- macro calculate_age_between_timestamps(ts1, ts2) -%}
    CAST(FLOOR(SAFE_DIVIDE(TIMESTAMP_DIFF({{ ts1 }}, {{ ts2 }}, HOUR), 24.0)) AS INT64)
{%- endmacro -%}

{# {%- macro calculate_age_between_timestamps(ts1, ts2) -%}
    CAST(FLOOR(SAFE_DIVIDE(TIMESTAMP_DIFF({{ ts1 }}, {{ ts2 }}, MINUTE), 1440.0)) AS INT64)
{%- endmacro -%} #}


{%- macro calculate_age_between_datetimes(dt1, dt2) -%}
    CAST(FLOOR(SAFE_DIVIDE(DATETIME_DIFF({{ dt1 }}, {{ dt2 }}, MINUTE), 1440.0)) AS INT64)
{%- endmacro -%}

