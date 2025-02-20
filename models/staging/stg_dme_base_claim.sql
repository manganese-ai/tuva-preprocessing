{{config(enabled=(not var('inpatient_only', False)))}}
{% call select_from_multi_source('dme') %}
      cast(BENE_ID as {{ dbt.type_string() }}) as DESY_SORT_KEY
    , cast(CLM_ID as {{ dbt.type_string() }}) as CLAIM_NO
    , cast(CARR_CLM_PMT_DNL_CD as {{ dbt.type_string() }}) as CARR_CLM_PMT_DNL_CD
    , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
    , cast(NCH_CLM_TYPE_CD as {{ dbt.type_string() }}) as NCH_CLM_TYPE_CD
    , cast(PRNCPAL_DGNS_CD as {{ dbt.type_string() }}) as PRNCPAL_DGNS_CD
    , cast(PRNCPAL_DGNS_VRSN_CD as {{ dbt.type_string() }}) as PRNCPAL_DGNS_VRSN_CD
    , cast(CARR_CLM_PRMRY_PYR_PD_AMT as {{ dbt.type_numeric() }}) as CARR_CLM_PRMRY_PYR_PD_AMT

    {% for diag in range(1,12) %}
    , cast(ICD_DGNS_CD{{ (diag+1) }} as {{ dbt.type_string() }}) as ICD_DGNS_CD{{ (diag+1) }}
    {% endfor %}
    

{% endcall %}