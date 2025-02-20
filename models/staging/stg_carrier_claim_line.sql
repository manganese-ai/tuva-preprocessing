{{config(enabled=(not var('inpatient_only', False)))}}
{% call select_from_multi_source('carrier_line') %}
      cast(CLM_ID as {{ dbt.type_string() }}) as CLAIM_NO
    , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
    , cast(HCPCS_1ST_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_1ST_MDFR_CD
    , cast(HCPCS_2ND_MDFR_CD as {{ dbt.type_string() }}) as HCPCS_2ND_MDFR_CD
    , cast(HCPCS_CD as {{ dbt.type_string() }}) as HCPCS_CD
    , cast(LINE_ALOWD_CHRG_AMT as {{ dbt.type_numeric() }}) as LINE_ALOWD_CHRG_AMT
    , cast(LINE_NCH_PMT_AMT as {{ dbt.type_numeric() }}) as LINE_NCH_PMT_AMT
    , cast(LINE_NUM as {{ dbt.type_numeric() }}) as LINE_NUM
    , cast(LINE_SRVC_CNT as {{ dbt.type_numeric() }}) as LINE_SRVC_CNT
    , cast(LINE_BENE_PTB_DDCTBL_AMT as {{ dbt.type_numeric() }}) as LINE_BENE_PTB_DDCTBL_AMT
    , cast(LINE_BENE_PRMRY_PYR_PD_AMT as {{ dbt.type_numeric() }}) as LINE_BENE_PRMRY_PYR_PD_AMT
    , cast(PRF_PHYSN_NPI as {{ dbt.type_string() }}) as PRF_PHYSN_NPI
    , lpad(cast(LINE_PLACE_OF_SRVC_CD AS STRING), 2, '0') as LINE_PLACE_OF_SRVC_CD

    /** new **/
    , {{ to_date("LINE_LAST_EXPNS_DT", 'yyyy-mm-dd') }} as LINE_LAST_EXPNS_DT

{% endcall %}