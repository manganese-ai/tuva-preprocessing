{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with pivot_procedure as(
    UNPIVOT {{ ref('normalized_input__stg_medical_claim') }}
    ON {% for i in range(1,26) %}
        procedure_code_{{i}}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    INTO
        NAME procedure_column
        VALUE procedure_code
)

select
    claim_id
    , data_source
    , procedure_code_type
    , procedure_column
    , coalesce(icd_10.icd_10_pcs) as normalized_procedure_code
    , count(*) as procedure_code_occurrence_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pivot_procedure piv
left join {{ ref('terminology__icd_10_pcs') }} icd_10
    on replace(piv.procedure_code,'.','') = icd_10.icd_10_pcs
where claim_type = 'institutional'
group by 
    claim_id
    , data_source
    , procedure_code_type
    , procedure_column
    , coalesce(icd_10.icd_10_pcs)