{{config(enabled=(not var('outpatient_only', False)))}}

with snf as (
  select
      cast(DESY_SORT_KEY as {{ dbt.type_string() }}) as DESY_SORT_KEY
    , cast(CLM_ID as {{ dbt.type_string() }}) as CLAIM_NO
    , {{ to_date("CLM_ADMSN_DT", 'yyyy-mm-dd') }} as CLM_ADMSN_DT
    , cast(CLM_DRG_CD as {{ dbt.type_string() }}) as CLM_DRG_CD
    , cast(CLM_FAC_TYPE_CD as {{ dbt.type_string() }}) as CLM_FAC_TYPE_CD
    , cast(CLM_FREQ_CD as {{ dbt.type_string() }}) as CLM_FREQ_CD
    , cast(CLM_PMT_AMT as {{ dbt.type_numeric() }}) as CLM_PMT_AMT
    , cast(CLM_SRC_IP_ADMSN_CD as {{ dbt.type_string() }}) as CLM_SRC_IP_ADMSN_CD
    , cast(CLM_SRVC_CLSFCTN_TYPE_CD as {{ dbt.type_string() }}) as CLM_SRVC_CLSFCTN_TYPE_CD
    , {{ to_date("CLM_THRU_DT", 'yyyy-mm-dd') }} as CLM_THRU_DT
    , cast(CLM_TOT_CHRG_AMT as {{ dbt.type_numeric() }}) as CLM_TOT_CHRG_AMT
    , cast(NCH_BENE_BLOOD_DDCTBL_LBLTY_AM as {{ dbt.type_numeric() }}) as NCH_BENE_BLOOD_DDCTBL_LBLTY_AM
    , {{ to_date("NCH_BENE_DSCHRG_DT", 'yyyy-mm-dd') }} as NCH_BENE_DSCHRG_DT
    , cast(NCH_BENE_IP_DDCTBL_AMT as {{ dbt.type_numeric() }}) as NCH_BENE_IP_DDCTBL_AMT
    , cast(NCH_BENE_PTA_COINSRNC_LBLTY_AM as {{ dbt.type_numeric() }}) as NCH_BENE_PTA_COINSRNC_LBLTY_AM
    , cast(NCH_CLM_TYPE_CD as {{ dbt.type_string() }}) as NCH_CLM_TYPE_CD
    , cast(NCH_PRMRY_PYR_CLM_PD_AMT as {{ dbt.type_numeric() }}) as NCH_PRMRY_PYR_CLM_PD_AMT
    , cast(ORG_NPI_NUM as {{ dbt.type_string() }}) as ORG_NPI_NUM
    , cast(PRNCPAL_DGNS_CD as {{ dbt.type_string() }}) as PRNCPAL_DGNS_CD
    , cast(PTNT_DSCHRG_STUS_CD as {{ dbt.type_string() }}) as PTNT_DSCHRG_STUS_CD
    , cast(RNDRNG_PHYSN_NPI as {{ dbt.type_string() }}) as RNDRNG_PHYSN_NPI
    , cast(NULL as {{ dbt.type_string() }} ) as CLM_MDCR_NON_PMT_RSN_CD

    {% for diag in range(1,25) %}
    , cast(ICD_DGNS_CD{{ (diag+1) }} as {{ dbt.type_string() }}) as ICD_DGNS_CD{{ (diag+1) }}
    {% endfor %}

    {% for proced in range(0,25) %}
    , cast(ICD_PRCDR_CD{{ (proced+1) }} as {{ dbt.type_string() }}) as ICD_PRCDR_CD{{ (proced+1) }}
    {% endfor %}

    {% for dt in range(0,25) %}
    , {{ to_date("PRCDR_DT" ~ (dt+1), 'yyyy-mm-dd') }} as PRCDR_DT{{ (dt+1) }}
    {% endfor %}

from {{ ref('stg_medpar') }}
where SS_LS_SNF_IND_CD in ('N')
)
select * from snf
inner join {{ ref('stg_eligibility') }} e
    on snf.DESY_SORT_KEY = e.patient_id

{{patient_id_subselect()}}