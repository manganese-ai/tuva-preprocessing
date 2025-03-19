{% macro patient_id_subselect() -%}
{% if var('patient_id_suffix', None) != None %}
WHERE desy_sort_key LIKE '%{{var("patient_id_suffix")}}'
{% endif %}
{% endmacro %}