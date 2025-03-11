/*  note from Tuva meeting: cost (and service categories): if leave blank, they'll just be part of 'Other' and still be part of medical paid */
{{config(enabled=(not var('outpatient_only', False)))}}

select * from (
  {% call select_from_multi_source('medpar') %}
        cast(BENE_ID as {{ dbt.type_string() }}) as DESY_SORT_KEY
      , ADMSN_DT as CLM_ADMSN_DT
      , cast(DRG_CD as {{ dbt.type_string() }}) as CLM_DRG_CD
      , cast(MDCR_PMT_AMT + PASS_THRU_AMT as {{ dbt.type_numeric() }}) as CLM_PMT_AMT
      , cast(SRC_IP_ADMSN_CD as {{ dbt.type_string() }}) as CLM_SRC_IP_ADMSN_CD
      , cast(TOT_CHRG_AMT as {{ dbt.type_numeric() }}) as CLM_TOT_CHRG_AMT
      , cast(DGNS_1_CD as {{ dbt.type_string() }}) as PRNCPAL_DGNS_CD
      , DSCHRG_DT as NCH_BENE_DSCHRG_DT
      , cast(BENE_DSCHRG_STUS_CD as {{ dbt.type_string() }}) as PTNT_DSCHRG_STUS_CD
      , cast(ORG_NPI_NUM as {{ dbt.type_string() }}) as ORG_NPI_NUM

      {% for diag in range(1,25) %}
      , cast(DGNS_{{ (diag+1) }}_CD as {{ dbt.type_string() }}) as ICD_DGNS_CD{{ (diag+1) }}
      {% endfor %}

      {% for proced in range(0,25) %}
      , cast(SRGCL_PRCDR_{{ (proced+1) }}_CD as {{ dbt.type_string() }}) as ICD_PRCDR_CD{{ (proced+1) }}
      {% endfor %}

      {% for dt in range(0,25) %}
      , "SRGCL_PRCDR_PRFRM_{{ dt+1 }}_DT" as PRCDR_DT{{ dt+1 }}
      {% endfor %}

      /* inpatient columns */
      , cast(IP_ADMSN_TYPE_CD as {{ dbt.type_string() }}) as CLM_IP_ADMSN_TYPE_CD

      {% for poa in range(0,25) %}
      , cast(POA_DGNS_{{ (poa+1) }}_IND_CD as {{ dbt.type_string() }}) as CLM_POA_IND_SW{{ (poa+1) }}
      {% endfor %}

      /* snf columns */
      , cast(POA_DGNS_E_1_IND_CD as {{ dbt.type_string() }}) as DGNS_E_1_CD
      , cast(BENE_BLOOD_DDCTBL_AMT as {{ dbt.type_numeric() }}) as NCH_BENE_BLOOD_DDCTBL_LBLTY_AM
      , cast(BENE_IP_DDCTBL_AMT as {{ dbt.type_numeric() }}) as NCH_BENE_IP_DDCTBL_AMT
      , cast(BENE_PTA_COINSRNC_AMT as {{ dbt.type_numeric() }}) as NCH_BENE_PTA_COINSRNC_LBLTY_AM
      , cast(BENE_PRMRY_PYR_AMT as {{ dbt.type_numeric() }}) as NCH_PRMRY_PYR_CLM_PD_AMT -- note that LDS specifies this for non-institutional claims only

      /** 
          medpar data missing columns needed for tuva
      **/

      /** medicare denied claims code **/
      -- Null, becuase none of the medpar claims have been denied (?)
      , cast(NULL as {{ dbt.type_string() }} ) as CLM_MDCR_NON_PMT_RSN_CD
      
      /** claim number **/
      , cast(MEDPAR_ID as {{ dbt.type_string() }}) as CLM_ID

      /** line number **/
      -- hard code 1 because everything is aggregated at header level
      , cast(1 as {{ dbt.type_numeric() }} ) as CLM_LINE_NUM

      /** claim thru date **/
      , case
        when DSCHRG_DT is not Null then cast(DSCHRG_DT as varchar)
        else cast(date_add(ADMSN_DT, CAST(LOS_DAY_CNT as integer)) as varchar)
        end as CLM_THRU_DT
    
      /** rendering physician npi **/
      -- make physician NPI as org NPI
      , cast(ORG_NPI_NUM as {{ dbt.type_string() }}) as RNDRNG_PHYSN_NPI

      /** claim type **/
      -- LDS (only listing relevant ones for MedPar)
          -- '20' = Non-swing bed SNF, '30' = Swing bed SNF, '60' = inpatient
      -- part of claim id, but nothing else (?)
      , case
          when cast(SS_LS_SNF_IND_CD as string) in ('S','L') then cast('60' as {{ dbt.type_string() }})
          when cast(SS_LS_SNF_IND_CD as string) = 'N' then cast('20' as {{ dbt.type_string() }})
          else null
        end as NCH_CLM_TYPE_CD

      /** bill type codes
          - clm_fac_type_cd, clm_srvc_clsfctn_type_cd, clm_freq_cd 
          - 111 for acute inpatient
          - 211 for snf
      **/

      /** facility code (one of 3 columns for institutional bills) **/
      , case
          when cast(SS_LS_SNF_IND_CD as string) in ('S','L') then cast('1' as {{ dbt.type_string() }}) -- inpatient
          when cast(SS_LS_SNF_IND_CD as string) = 'N' then cast('2' as {{ dbt.type_string() }}) -- snf
          else null
        end as CLM_FAC_TYPE_CD

      /** claim service classification code (one of 3 columns for institutional bills) **/
      , cast('1' as {{ dbt.type_string() }}) as CLM_SRVC_CLSFCTN_TYPE_CD

      /** claim frequency code (one of 3 columns for institutional bills) **/
      -- Tuva doesn't do anything with this column
      , cast('1' as {{ dbt.type_string() }}) as CLM_FREQ_CD

      /** renenue center code **/
      -- hard code 0001
      , cast('0001' as {{ dbt.type_string() }}) as REV_CNTR

      /** renenue center unit count **/
      -- won't affect any downstream things (according to Tuva)
      , cast(0 as {{ dbt.type_numeric() }}) as REV_CNTR_UNIT_CNT
      
      /** hcpcs **/
      -- not needed for the Tuva marts we are using
      , cast(null as {{ dbt.type_string() }}) as HCPCS_CD
      , cast(null as {{ dbt.type_string() }}) as HCPCS_1ST_MDFR_CD
      , cast(null as {{ dbt.type_string() }}) as HCPCS_2ND_MDFR_CD
      , cast(null as {{ dbt.type_string() }}) as HCPCS_3RD_MDFR_CD

      /** needed to differentiate snf / inpatient **/
      , cast(SS_LS_SNF_IND_CD as {{ dbt.type_string() }}) as SS_LS_SNF_IND_CD

  {% endcall %}
) m

inner join {{ ref('stg_eligibility') }} e
    on m.DESY_SORT_KEY = e.patient_id

{{patient_id_subselect()}}