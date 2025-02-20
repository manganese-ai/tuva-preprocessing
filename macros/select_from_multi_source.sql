{% macro select_from_multi_source(tbl, suffixes=None) -%}
{% if suffixes is none -%}
{% set suffixes = var('years', 'all').split(',') -%}
{% endif -%}
({% for s in suffixes %}
    SELECT {{ caller() }} FROM {{source('medicare_lds', tbl+'_'+s)}}
    {% if not loop.last %}UNION{% endif %}
{% endfor %})
{%- endmacro %}