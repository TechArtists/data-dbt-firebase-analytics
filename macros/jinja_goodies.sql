{%- macro list_map_and_add_prefix(list, prefix) -%}
{%- set res = [] %}
{%- for elem in list -%}
  {%- if elem is none -%}
    {%- set _ = res.append("NULL") -%}
  {%- else -%}
    {%- set _ = res.append(prefix ~ elem) -%}
  {%- endif -%}
{%- endfor -%}
{{ return(res) }}
{%- endmacro -%}

{%- macro list_map_and_add_suffix(list, suffix) -%}
{%- set res = [] %}
{%- for elem in list -%}
  {%- if elem is none -%}
    {%- set _ = res.append("NULL") -%}
  {%- else -%}
    {%- set _ = res.append(elem ~ suffix) -%}
  {%- endif -%}
{%- endfor -%}
{{ return(res) }}
{%- endmacro -%}

{# So event_name ["foo", "bar"] becomes the string: event_name IN ("foo", "bar"). 
If the list is empty, it returns just True
It also looks for any values starting with "LIKE" and returns (event_name IN ('foo') OR event_name LIKE 'bar%')
 #}
{%- macro makeListIntoSQLInFilter(sqlField, myList) -%}
  {%- set myListOfExactValues = [] -%}
  {%- set myListOfLikes = [] -%}
  {%- for value in myList -%}
    {%- if value.lower().startswith("like ") -%}
      {%- set _ = myListOfLikes.append(sqlField ~ " " ~ value) -%}
    {%- else -%}
      {%- set _ = myListOfExactValues.append(value) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if myList | length == 0 -%}
    True
  {%- else -%}
    (
      {%- if myListOfExactValues | length > 0 -%}
        {{ sqlField }} IN {{ tojson(myListOfExactValues).replace("[", "(").replace("]", ")") }}
      {%- endif -%}
      {%- if myListOfExactValues | length > 0 and myListOfLikes | length > 0 %} OR {% endif -%}
      {%- if myListOfLikes | length > 0 -%}
        {{ myListOfLikes | join(" OR ") }}
      {%- endif -%}
    )
  {%- endif -%}
{%- endmacro -%}



{# So event_name ["foo", "bar"] becomes the string: event_name IN ("foo", "bar"). 
If the list is empty, it returns just True
 #}
{%- macro flatten_list_of_lists(myListOfLists) -%}
  {%- set res = [] %}
  {%- for myList in myListOfLists -%}
    {%- for elem in myList -%}
      {%- set _ = res.append(elem) -%}    
    {%- endfor -%}
  {%- endfor -%}
  {{ return(res) }}
{%- endmacro -%}