{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with all_procedures as (
{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_procedure') }}
union all
select * from {{ ref('core__stg_clinical_procedure') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_procedure') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_procedure') }}

{%- endif %}
)

select
    all_procedures.procedure_id
  , all_procedures.person_id
  , all_procedures.member_id
  , all_procedures.patient_id
  , all_procedures.encounter_id
  , all_procedures.claim_id
  , all_procedures.procedure_date
  , all_procedures.source_code_type
  , all_procedures.source_code
  , all_procedures.source_description
  , case
      when all_procedures.normalized_code_type is not null then all_procedures.normalized_code_type
      when icd10.icd_10_pcs is not null then 'icd-10-pcs'
      when hcpcs.hcpcs is not null then 'hcpcs'
    end as normalized_code_type
  , case 
      when all_procedures.source_code_type = 'icd-10-pcs'
        then coalesce(all_procedures.normalized_code, icd10.icd_10_pcs)
      when all_procedures.source_code_type = 'hcpcs'
        then coalesce(all_procedures.normalized_code, hcpcs.hcpcs)
    end as normalized_code
  , case 
      when all_procedures.source_code_type = 'icd-10-pcs'
        then coalesce(all_procedures.normalized_description, icd10.description)
      when all_procedures.source_code_type = 'hcpcs'
        then coalesce(all_procedures.normalized_description, hcpcs.short_description)
    end as normalized_description
  , case
      when coalesce(all_procedures.normalized_code, all_procedures.normalized_description) is not null then 'manual'
      when coalesce(icd10.icd_10_pcs, hcpcs.hcpcs) is not null then 'automatic'
    end as mapping_method
  , all_procedures.modifier_1
  , all_procedures.modifier_2
  , all_procedures.modifier_3
  , all_procedures.modifier_4
  , all_procedures.modifier_5
  , all_procedures.practitioner_id
  , all_procedures.data_source
  , all_procedures.tuva_last_run
from all_procedures
left join {{ ref('terminology__icd_10_pcs') }} icd10
    on all_procedures.source_code = icd10.icd_10_pcs
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs
    on all_procedures.source_code = hcpcs.hcpcs