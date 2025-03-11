{{config(enabled=(not var('inpatient_only', False)))}}

select * from (
  {% call select_from_multi_source('hha') %}
        cast(BENE_ID as {{ dbt.type_string() }}) as DESY_SORT_KEY
      , cast(CLM_ID as {{ dbt.type_string() }}) as CLAIM_NO
      , {{ to_date("CLM_ADMSN_DT", 'yyyy-mm-dd') }} as CLM_ADMSN_DT
      , cast(CLM_FAC_TYPE_CD as {{ dbt.type_string() }}) as CLM_FAC_TYPE_CD
      , cast(CLM_FREQ_CD as {{ dbt.type_string() }}) as CLM_FREQ_CD
      , cast(CLM_PMT_AMT as {{ dbt.type_numeric() }}) as CLM_PMT_AMT
      , cast(CLM_SRVC_CLSFCTN_TYPE_CD as {{ dbt.type_string() }}) as CLM_SRVC_CLSFCTN_TYPE_CD
      , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
      , cast(CLM_TOT_CHRG_AMT as {{ dbt.type_numeric() }}) as CLM_TOT_CHRG_AMT
      , cast(NCH_CLM_TYPE_CD as {{ dbt.type_string() }}) as NCH_CLM_TYPE_CD
      , cast(NCH_PRMRY_PYR_CLM_PD_AMT as {{ dbt.type_numeric() }}) as NCH_PRMRY_PYR_CLM_PD_AMT
      , cast(ORG_NPI_NUM as {{ dbt.type_string() }}) as ORG_NPI_NUM
      , cast(PRNCPAL_DGNS_CD as {{ dbt.type_string() }}) as PRNCPAL_DGNS_CD
      , cast(PTNT_DSCHRG_STUS_CD as {{ dbt.type_string() }}) as PTNT_DSCHRG_STUS_CD
      , case 
          when CLM_MDCR_NON_PMT_RSN_CD = ' ' then NULL 
          else cast(CLM_MDCR_NON_PMT_RSN_CD as {{ dbt.type_string() }})
        end as CLM_MDCR_NON_PMT_RSN_CD
      , {{ to_date("NCH_BENE_DSCHRG_DT", 'yyyy-mm-dd') }} as NCH_BENE_DSCHRG_DT
      , cast(SRVC_LOC_NPI_NUM as {{ dbt.type_string() }}) as SRVC_LOC_NPI_NUM

      {% for diag in range(1,25) %}
      , cast(ICD_DGNS_CD{{ (diag+1) }} as {{ dbt.type_string() }}) as ICD_DGNS_CD{{ (diag+1) }}
      {% endfor %}

  {% endcall %}
) m

inner join {{ ref('stg_eligibility') }} e
    on m.DESY_SORT_KEY = e.patient_id

{{patient_id_subselect()}}