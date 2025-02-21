-- this is modified from the tuva one

with eligibility as (
    select patient_id
    from {{ ref('eligibility') }}
)

, unioned as (

    {{ dbt_utils.union_relations(relations=[
          ref('carrier_claim')
        , ref('dme_claim')
        , ref('home_health_claim')
        , ref('hospice_claim')
        , ref('inpatient_claim')
        , ref('outpatient_claim')
        , ref('snf_claim')
    ],
    exclude=["_DBT_SOURCE_RELATION"]
) }}
)

select
      u.claim_id
    , u.claim_line_number
    , u.claim_type
    , u.person_id
    , u.member_id
    , u.payer
    , u.plan
    , u.claim_start_date
    , u.claim_end_date
    , u.claim_line_start_date
    , u.claim_line_end_date
    , u.admission_date
    , u.discharge_date
    , u.admit_source_code
    , u.admit_type_code
    , u.discharge_disposition_code
    , u.place_of_service_code
    , u.bill_type_code
    , u.ms_drg_code
    , u.apr_drg_code
    , u.revenue_center_code
    , u.service_unit_quantity
    , u.hcpcs_code
    , u.hcpcs_modifier_1
    , u.hcpcs_modifier_2
    , u.hcpcs_modifier_3
    , u.hcpcs_modifier_4
    , u.hcpcs_modifier_5
    , u.rendering_npi
    , u.rendering_tin
    , u.billing_npi
    , u.billing_tin
    , u.facility_npi
    , u.paid_date
    , u.paid_amount
    , u.allowed_amount
    , u.charge_amount
    , u.coinsurance_amount
    , u.copayment_amount
    , u.deductible_amount
    , u.total_cost_amount
    , u.diagnosis_code_type
    , u.diagnosis_code_1
    , u.diagnosis_code_2
    , u.diagnosis_code_3
    , u.diagnosis_code_4
    , u.diagnosis_code_5
    , u.diagnosis_code_6
    , u.diagnosis_code_7
    , u.diagnosis_code_8
    , u.diagnosis_code_9
    , u.diagnosis_code_10
    , u.diagnosis_code_11
    , u.diagnosis_code_12
    , u.diagnosis_code_13
    , u.diagnosis_code_14
    , u.diagnosis_code_15
    , u.diagnosis_code_16
    , u.diagnosis_code_17
    , u.diagnosis_code_18
    , u.diagnosis_code_19
    , u.diagnosis_code_20
    , u.diagnosis_code_21
    , u.diagnosis_code_22
    , u.diagnosis_code_23
    , u.diagnosis_code_24
    , u.diagnosis_code_25
    , u.diagnosis_poa_1
    , u.diagnosis_poa_2
    , u.diagnosis_poa_3
    , u.diagnosis_poa_4
    , u.diagnosis_poa_5
    , u.diagnosis_poa_6
    , u.diagnosis_poa_7
    , u.diagnosis_poa_8
    , u.diagnosis_poa_9
    , u.diagnosis_poa_10
    , u.diagnosis_poa_11
    , u.diagnosis_poa_12
    , u.diagnosis_poa_13
    , u.diagnosis_poa_14
    , u.diagnosis_poa_15
    , u.diagnosis_poa_16
    , u.diagnosis_poa_17
    , u.diagnosis_poa_18
    , u.diagnosis_poa_19
    , u.diagnosis_poa_20
    , u.diagnosis_poa_21
    , u.diagnosis_poa_22
    , u.diagnosis_poa_23
    , u.diagnosis_poa_24
    , u.diagnosis_poa_25
    , u.procedure_code_type
    , u.procedure_code_1
    , u.procedure_code_2
    , u.procedure_code_3
    , u.procedure_code_4
    , u.procedure_code_5
    , u.procedure_code_6
    , u.procedure_code_7
    , u.procedure_code_8
    , u.procedure_code_9
    , u.procedure_code_10
    , u.procedure_code_11
    , u.procedure_code_12
    , u.procedure_code_13
    , u.procedure_code_14
    , u.procedure_code_15
    , u.procedure_code_16
    , u.procedure_code_17
    , u.procedure_code_18
    , u.procedure_code_19
    , u.procedure_code_20
    , u.procedure_code_21
    , u.procedure_code_22
    , u.procedure_code_23
    , u.procedure_code_24
    , u.procedure_code_25
    , u.procedure_date_1
    , u.procedure_date_2
    , u.procedure_date_3
    , u.procedure_date_4
    , u.procedure_date_5
    , u.procedure_date_6
    , u.procedure_date_7
    , u.procedure_date_8
    , u.procedure_date_9
    , u.procedure_date_10
    , u.procedure_date_11
    , u.procedure_date_12
    , u.procedure_date_13
    , u.procedure_date_14
    , u.procedure_date_15
    , u.procedure_date_16
    , u.procedure_date_17
    , u.procedure_date_18
    , u.procedure_date_19
    , u.procedure_date_20
    , u.procedure_date_21
    , u.procedure_date_22
    , u.procedure_date_23
    , u.procedure_date_24
    , u.procedure_date_25
    , u.in_network_flag
from unioned as u
    inner join eligibility as e
        on u.person_id = e.patient_id