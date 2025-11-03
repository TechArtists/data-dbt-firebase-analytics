{%- macro null_if_length_zero(columnName) -%}
  IF(LENGTH( {{ columnName }} ) > 0, {{ columnName }}, NULL)
{%- endmacro -%}