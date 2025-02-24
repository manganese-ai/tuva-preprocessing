{% macro patient_id_subselect() -%}
{% if var('patient_id_suffix', None) != None %}
WHERE person_id LIKE '%{{var("patient_id_suffix")}}'
{% endif %}
{% endmacro %}