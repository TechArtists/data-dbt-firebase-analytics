{%- macro analyticsTestDateFilter(fieldName, extend = 0) -%}
    {{ ccDateFilterFor(fieldName, forceIncremental = True, extend = extend) }} 
{%- endmacro %}

{%- macro analyticsTestTableSuffixFilter(extend =0) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple(forceIncremental = True, extend=extend + 1) -%}  {# extended by one day because TABLE_SUFFIX is not always UTC #}
  REPLACE(_TABLE_SUFFIX, 'intraday_', '') BETWEEN FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[0] }}) AND FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro analyticsDateFilterFor(dateField,extend = 0) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple(extend = extend) -%}
	{{ dateField }} BETWEEN DATE({{ startEndTSTuple[0] }}) AND DATE({{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro crashlyticsDateFilterFor(dateField, extend = 0) -%}
	{%- set startEndTSTuple = overbase_firebase.crashlyticsStartEndTimestampsTuple(extend=extend) -%}
	{{ dateField }} BETWEEN DATE({{ startEndTSTuple[0] }}) AND DATE({{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro analyticsTSFilterFor(tsField, extend = 0) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple(extend=extend) -%}
	{{ tsField }} BETWEEN {{ startEndTSTuple[0] }} AND {{ startEndTSTuple[1] }}	
{%- endmacro -%}

{%- macro crashlyticsTSFilterFor(tsField, extend = 0) -%}
	{%- set startEndTSTuple = overbase_firebase.crashlyticsStartEndTimestampsTuple(extend=extend) -%}
	{{ tsField }} BETWEEN {{ startEndTSTuple[0] }} AND {{ startEndTSTuple[1] }}	
{%- endmacro -%}

{%- macro analyticsTableSuffixFilter(extend =0) -%}
	{%- set startEndTSTuple = overbase_firebase.analyticsStartEndTimestampsTuple(extend=extend + 1) -%}  {# extended by one day because TABLE_SUFFIX is not always UTC #}
  REPLACE(_TABLE_SUFFIX, 'intraday_', '') BETWEEN FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[0] }}) AND FORMAT_DATE('%Y%m%d', {{ startEndTSTuple[1] }})
{%- endmacro -%}

{%- macro analyticsStartEndTimestampsTuple(forceIncremental = False, extend = 0) -%}
	{%- if (forceIncremental or is_incremental()) -%}
        {%- set INCREMENTAL_DAYS =  var('OVERBASE:FIREBASE_ANALYTICS_DEFAULT_INCREMENTAL_DAYS', 5 ) +extend  -%}
		{%- set tsStart = "TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL " ~  INCREMENTAL_DAYS  ~ " DAY)" -%}
		{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{{ return((tsStart, tsEnd)) }}
	{%- else -%}
		{%- set tsStart = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE', '2018-01-01') ~ "')" -%}
		{%- if var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_END_DATE', "nada") == 'nada' -%}
			{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{%- else -%}
			{%- set tsEnd = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_END_DATE', "")  ~ "')" -%}
		{%- endif -%}
		{{ return((tsStart, tsEnd)) }}
	{%- endif -%}
{%- endmacro -%}


{%- macro crashlyticsStartEndTimestampsTuple(extend = 0) -%}
	{%- if is_incremental() -%}
        {%- set INCREMENTAL_DAYS =  5 + extend -%}
		{%- set tsStart = "TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL " ~ var('OVERBASE:FIREBASE_CRASHLYTICS_DEFAULT_INCREMENTAL_DAYS',INCREMENTAL_DAYS)  ~ " DAY)" -%}
		{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{{ return((tsStart, tsEnd)) }}
	{%- else -%}
		{%- set tsStart = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_START_DATE', '2018-01-01') ~ "')" -%}
		{%- if var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_END_DATE', "nada") == 'nada' -%}
			{%- set tsEnd = "TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))" -%}
		{%- else -%}
			{%- set tsEnd = "TIMESTAMP('" ~ var('OVERBASE:FIREBASE_CRASHLYTICS_FULL_REFRESH_END_DATE', "")  ~ "')" -%}
		{%- endif -%}
		{{ return((tsStart, tsEnd)) }}
	{%- endif -%}
{%- endmacro -%}

