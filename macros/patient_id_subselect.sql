{% macro patient_id_subselect() -%}
{% if var('patient_id_suffix', None) != None %}
WHERE patient_id LIKE '%{{var("patient_id_suffix")}}'
{% endif %}
{% endmacro %}