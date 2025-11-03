{%- macro install_age_group(ageField) -%}
  CASE  WHEN {{ ageField }} =  0 THEN 'Age 0'
        WHEN {{ ageField }} <=  6 THEN 'Age 1-6'
        WHEN {{ ageField }} <= 30 THEN 'Age 7-30'
        ELSE 'Age 31+'
  END
{%- endmacro -%}