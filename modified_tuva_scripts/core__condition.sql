{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with all_conditions as (
{% if var('clinical_enabled', var('tuva_marts_enabled', False)) == true
    and var('claims_enabled', var('tuva_marts_enabled', False)) == true -%}

    select *
    from {{ ref('core__stg_claims_condition') }}
    union all
    select *
    from {{ ref('core__stg_clinical_condition') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_clinical_condition') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_claims_condition') }}

{%- endif %}
)

select
    all_conditions.condition_id
  , all_conditions.person_id
  , all_conditions.member_id
  , all_conditions.patient_id
  , all_conditions.encounter_id
  , all_conditions.claim_id
  , all_conditions.recorded_date
  , all_conditions.onset_date
  , all_conditions.resolved_date
  , all_conditions.status
  , all_conditions.condition_type
  , all_conditions.source_code_type
  , all_conditions.source_code
  , all_conditions.source_description
  , case
        when all_conditions.normalized_code_type is not null then all_conditions.normalized_code_type
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        else null end as normalized_code_type
  , coalesce(
        all_conditions.normalized_code
      , icd10.icd_10_cm) as normalized_code
  , coalesce(
        all_conditions.normalized_description
      , icd10.short_description) as normalized_description
  , case when coalesce(all_conditions.normalized_code, all_conditions.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_cm) is not null then 'automatic'
         end as mapping_method
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
  , all_conditions.data_source
  , all_conditions.tuva_last_run
from
all_conditions
left join {{ ref('terminology__icd_10_cm') }} icd10
    on replace(all_conditions.source_code,'.','') = icd10.icd_10_cm