{{config(enabled=(not var('inpatient_only', False)))}}
{% call select_from_multi_source('outpatient_revs') %}
      cast(CLM_ID as {{ dbt.type_string() }} ) as CLAIM_NO
    , cast(CLM_LINE_NUM as {{ dbt.type_numeric() }}) as CLM_LINE_NUM
    , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
    , cast(HCPCS_1ST_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_1ST_MDFR_CD
    , cast(HCPCS_2ND_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_2ND_MDFR_CD
    , cast(HCPCS_CD as {{ dbt.type_string() }}) as HCPCS_CD
    , {{ to_date("REV_CNTR_DT", 'yyyy-mm-dd') }} as REV_CNTR_DT
    , cast(REV_CNTR_UNIT_CNT as {{ dbt.type_numeric() }}) as REV_CNTR_UNIT_CNT
    , lpad(CAST(REV_CNTR AS STRING), 4, '0') as REV_CNTR
    , cast(RNDRNG_PHYSN_NPI as {{ dbt.type_string() }}) as REV_CNTR_RNDRNG_PHYSN_NPI
    , cast(HCPCS_3RD_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_3RD_MDFR_CD
    , cast(HCPCS_4TH_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_4TH_MDFR_CD

{% endcall %}