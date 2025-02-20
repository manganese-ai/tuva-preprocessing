{% call select_from_multi_source('beneficiary') %}
      cast(BENE_ID as {{ dbt.type_string() }}) as DESY_SORT_KEY
    , cast(BENE_ENROLLMT_REF_YR as {{ dbt.type_numeric() }}) as REFERENCE_YEAR
    , cast(AGE_AT_END_REF_YR as {{ dbt.type_numeric() }}) as AGE
    , cast(SEX_IDENT_CD as {{ dbt.type_string() }}) as SEX_CODE
    , cast(BENE_RACE_CD as {{ dbt.type_string() }}) as RACE_CODE
    , cast(ENTLMT_RSN_ORIG as {{ dbt.type_string() }}) as ORIG_REASON_FOR_ENTITLEMENT
    , cast(BENE_HI_CVRAGE_TOT_MONS as {{ dbt.type_numeric() }}) as HI_COVERAGE
    , cast(BENE_SMI_CVRAGE_TOT_MONS as {{ dbt.type_numeric() }}) as SMI_COVERAGE
    , cast(BENE_HMO_CVRAGE_TOT_MONS as {{ dbt.type_numeric() }}) as HMO_COVERAGE
    , {{ to_date("BENE_DEATH_DT", 'yyyy-mm-dd') }} as DATE_OF_DEATH
    , cast(STATE_CODE as {{ dbt.type_string() }}) as STATE_CODE

    {% for month in range(0,12) %}
    , case 
        when DUAL_STUS_CD_{{"%02d"|format(month+1)}} in ('None',null) then null 
        when DUAL_STUS_CD_{{"%02d"|format(month+1)}} in ('NA') then 'NA' 
        else lpad(cast(DUAL_STUS_CD_{{"%02d"|format(month+1)}} AS STRING), 2, '0')
      end as DUAL_STUS_CD_{{"%02d"|format(month+1)}}
    {% endfor %}

    {% for month in range(0,12) %}
    ,  case 
        when MDCR_STATUS_CODE_{{"%02d"|format(month+1)}} in ('None','NA','NULL',null) then null 
        else cast(MDCR_STATUS_CODE_{{"%02d"|format(month+1)}} as {{ dbt.type_string() }})
      end as MDCR_STATUS_CODE_{{"%02d"|format(month+1)}}
    {% endfor %}


{% endcall %}