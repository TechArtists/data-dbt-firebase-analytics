{# STRUCT<minus_12 DATE , minus_11 DATE , minus_10 DATE , minus_9 DATE , minus_8 DATE , minus_7 DATE , minus_6 DATE , minus_5 DATE , minus_4 DATE , minus_3 DATE , minus_2 DATE , minus_1 DATE , plus_1 DATE , plus_2 DATE , plus_3 DATE , plus_4 DATE , plus_5 DATE , plus_6 DATE , plus_7 DATE , plus_8 DATE , plus_9 DATE , plus_10 DATE , plus_11 DATE , plus_12 DATE >(
     DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-12:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+12:00')) 
#}
{%- macro generate_date_timezone_struct(tsField) -%}
    STRUCT<
    {%- for timezoneTuple in ta_firebase.generate_datetime_timezone_tuple(tsField) -%}
            {{ timezoneTuple[0] }} DATE {{ ", " if not loop.last else "" }}
    {%- endfor -%}>(
    {% for timezoneTuple in ta_firebase.generate_datetime_timezone_tuple(tsField) -%}
            DATE({{ timezoneTuple[1] }})  {{ ", " if not loop.last else "" }}
    {%- endfor -%}
    )
{% endmacro -%}

{#  STRUCT<minus_12 DATE , minus_11 DATE , minus_10 DATE , minus_9 DATE , minus_8 DATE , minus_7 DATE , minus_6 DATE , minus_5 DATE , minus_4 DATE , minus_3 DATE , minus_2 DATE , minus_1 DATE , plus_1 DATE , plus_2 DATE , plus_3 DATE , plus_4 DATE , plus_5 DATE , plus_6 DATE , plus_7 DATE , plus_8 DATE , plus_9 DATE , plus_10 DATE , plus_11 DATE , plus_12 DATE >(
    CAST(FLOOR(SAFE_DIVIDE(DATETIME_DIFF(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-12:00')
                                       , DATETIME(TIMESTAMP_MICROS(event_timestamp), '-12:00')
                            , MINUTE), 1440.0)) AS INT64)

Turns out this isn't acutally needed, because the install_age in UTC, that takes into account the first 24h would yield the same value in all timezones.
  {{ ta_firebase.generate_date_timezone_age_struct('TIMESTAMP_MICROS(event_timestamp)', 'TIMESTAMP_MICROS(user_first_touch_timestamp)') }} as install_ages 
#}
{%- macro generate_date_timezone_age_struct(tsField1, tsField2) -%}
    STRUCT<
    {%- for timezoneTuple in ta_firebase.generate_datetime_timezone_tuple(tsField1) -%}
            {{ timezoneTuple[0] }} INT64 {{ ", " if not loop.last else "" }}
    {%- endfor -%}>(
    {% set datetimesField1 = ta_firebase.generate_datetime_timezone_tuple(tsField1) | map(attribute=1) | list %}
    {%- set datetimesField2 = ta_firebase.generate_datetime_timezone_tuple(tsField2) | map(attribute=1) | list %}
    {%- set datetimeFields = zip(datetimesField1, datetimesField2) -%}
    {%- for datetimes in datetimeFields -%}
        {{ ta_firebase.calculate_age_between_datetimes(datetimes[0], datetimes[1])  }} {{ ", " if not loop.last else "" }}
    {%- endfor -%}
    )
{% endmacro -%}

{# [(struct_field_name, extratction)]
    e.g. [('minus_12', "DATETIME($tsField), '12:00')" )]
 #}
{%- macro generate_datetime_timezone_tuple(tsField) -%}
    {%- set res = [] -%}
    {%- for n in range(-12, 13) -%}
        {%- if n < 0 -%}
            {%- set _ = res.append( ( 'minus_' ~ n|abs, "DATETIME(" ~ tsField ~ ", '-" ~   '%02d' % n|abs ~ ":00')" ) ) -%}
            {%- elif n > 0 -%}
            {%- set _ = res.append( ( 'plus_'  ~ n|abs, "DATETIME(" ~ tsField ~ ", '+" ~   '%02d' % n|abs ~ ":00')" ) ) -%}
        {%- endif -%}
    {% endfor -%}>
    {{ return(res) }}
{%- endmacro -%}