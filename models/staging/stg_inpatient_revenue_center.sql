{{config(enabled=(not var('outpatient_only', False)))}}

with inpatient as (
  select
      cast(DESY_SORT_KEY as {{ dbt.type_string() }}) as DESY_SORT_KEY
    , cast(CLM_ID as {{ dbt.type_string() }}) as CLAIM_NO
    , cast(CLM_LINE_NUM as {{ dbt.type_numeric() }}) as CLM_LINE_NUM
    , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
    , cast(HCPCS_CD as {{ dbt.type_string() }}) as HCPCS_CD
    , lpad(CAST(REV_CNTR AS STRING), 4, '0') as REV_CNTR
    , cast(0 as {{ dbt.type_numeric() }}) as REV_CNTR_UNIT_CNT

    /** missing in medpar **/
    , cast(null as {{ dbt.type_string() }}) as HCPCS_1ST_MDFR_CD
    , cast(null as {{ dbt.type_string() }}) as HCPCS_2ND_MDFR_CD
    , cast(null as {{ dbt.type_string() }}) as HCPCS_3RD_MDFR_CD
    , cast(null as {{ dbt.type_string() }}) as RNDRNG_PHYSN_NPI

from {{ ref('stg_medpar') }}
where SS_LS_SNF_IND_CD in ('L','S')
)
select * from inpatient
inner join {{ ref('stg_eligibility') }} e
    on inpatient.DESY_SORT_KEY = e.patient_id

{{patient_id_subselect()}}