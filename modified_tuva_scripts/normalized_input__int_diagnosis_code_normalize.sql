{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with pivot_diagnosis as(
    UNPIVOT {{ ref('normalized_input__stg_medical_claim') }}
    ON {% for i in range(1,26) %}
        diagnosis_code_{{i}}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    INTO
        NAME diagnosis_column
        VALUE diagnosis_code
)

select
    claim_id
    , data_source
    , diagnosis_code_type
    , diagnosis_column
    , coalesce(icd_10.icd_10_cm) as normalized_diagnosis_code
    , count(*) as diagnosis_code_occurrence_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pivot_diagnosis piv
left join {{ ref('terminology__icd_10_cm') }} icd_10
    on replace(piv.diagnosis_code,'.','') = icd_10.icd_10_cm
where claim_type <> 'undetermined'
group by 
    claim_id
    , data_source
    , diagnosis_code_type
    , diagnosis_column
    , coalesce(icd_10.icd_10_cm)
