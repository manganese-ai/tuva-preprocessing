with hospice_base_claim as (

    select *
         , left({{ cast_string_or_varchar('clm_thru_dt') }},4) as clm_thru_dt_year
    from {{ ref('stg_hospice_base_claim') }}
    where clm_mdcr_non_pmt_rsn_cd is null
    /** filter out denied claims **/

)

, header_payment as (

    select
          claim_no as claim_id
        , cast(clm_pmt_amt as {{ dbt.type_numeric() }}) as paid_amount
        , /** medicare payment **/
          cast(clm_pmt_amt as {{ dbt.type_numeric() }})
              /** primary payer payment **/
              + cast(nch_prmry_pyr_clm_pd_amt as {{ dbt.type_numeric() }})
          as total_cost_amount
        , cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }}) as charge_amount
    from hospice_base_claim

)

, claim_start_date as (

    select
          claim_no
        , min(rev_cntr_dt) as claim_start_date
    from {{ ref('stg_hospice_revenue_center') }}
    group by claim_no

)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      cast(b.claim_no as {{ dbt.type_string() }} )
        || cast(b.clm_thru_dt_year as {{ dbt.type_string() }} )
        || cast(b.nch_clm_type_cd as {{ dbt.type_string() }} )
      as claim_id
    , cast(l.clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , cast('hospice' as {{ dbt.type_string() }} ) as claim_category
    , cast(b.desy_sort_key as {{ dbt.type_string() }} ) as person_id
    , cast(b.desy_sort_key as {{ dbt.type_string() }} ) as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , {{ try_to_cast_date('coalesce(c.claim_start_date,b.clm_hospc_start_dt_id,b.clm_thru_dt)', 'YYYYMMDD') }} as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_end_date
    , {{ try_to_cast_date('l.rev_cntr_dt', 'YYYYMMDD') }} as claim_line_start_date
    , {{ try_to_cast_date('l.rev_cntr_dt', 'YYYYMMDD') }} as claim_line_end_date
    , {{ try_to_cast_date('b.clm_hospc_start_dt_id', 'YYYYMMDD') }} as admission_date
    , {{ try_to_cast_date('b.nch_bene_dschrg_dt', 'YYYYMMDD') }} as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(b.clm_fac_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_freq_cd as {{ dbt.type_string() }} )
      as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(l.rev_cntr as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(regexp_extract(cast(l.rev_cntr_unit_cnt as varchar),'.') as integer) as service_unit_quantity
    , cast(l.hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(l.hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(l.hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(l.hcpcs_3rd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(l.rev_cntr_rndrng_physn_npi as {{ dbt.type_string() }} ) as rendering_npi
    , cast(NULL as {{ dbt.type_string() }} ) as rendering_tin
    , cast(b.org_npi_num as {{ dbt.type_string() }} ) as billing_npi
    , cast(NULL as {{ dbt.type_string() }} ) as billing_tin
    , cast(coalesce(b.org_npi_num,b.srvc_loc_npi_num) as {{ dbt.type_string() }} ) as facility_npi
    , cast(NULL as date) as paid_date
    , cast(NULL as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(null as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(null as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(null as {{ dbt.type_numeric() }}) as deductible_amount
    , case when l.rev_cntr = '0001' 
          then p.paid_amount 
          else NULL 
      end as paid_amount
    , case when l.rev_cntr = '0001' 
          then p.charge_amount 
          else NULL 
      end as charge_amount
    , case when l.rev_cntr = '0001' 
          then p.total_cost_amount
          else NULL 
      end as total_cost_amount
    , 'icd-10-cm' as diagnosis_code_type
    , cast(b.prncpal_dgns_cd as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(b.icd_dgns_cd2 as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(b.icd_dgns_cd3 as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(b.icd_dgns_cd4 as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(b.icd_dgns_cd5 as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(b.icd_dgns_cd6 as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(b.icd_dgns_cd7 as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(b.icd_dgns_cd8 as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(b.icd_dgns_cd9 as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(b.icd_dgns_cd10 as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(b.icd_dgns_cd11 as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(b.icd_dgns_cd12 as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(b.icd_dgns_cd13 as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(b.icd_dgns_cd14 as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(b.icd_dgns_cd15 as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(b.icd_dgns_cd16 as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(b.icd_dgns_cd17 as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(b.icd_dgns_cd18 as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(b.icd_dgns_cd19 as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(b.icd_dgns_cd20 as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(b.icd_dgns_cd21 as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(b.icd_dgns_cd22 as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(b.icd_dgns_cd23 as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(b.icd_dgns_cd24 as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(b.icd_dgns_cd25 as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_25
    , cast(NULL as date) as procedure_date_1
    , cast(NULL as date) as procedure_date_2
    , cast(NULL as date) as procedure_date_3
    , cast(NULL as date) as procedure_date_4
    , cast(NULL as date) as procedure_date_5
    , cast(NULL as date) as procedure_date_6
    , cast(NULL as date) as procedure_date_7
    , cast(NULL as date) as procedure_date_8
    , cast(NULL as date) as procedure_date_9
    , cast(NULL as date) as procedure_date_10
    , cast(NULL as date) as procedure_date_11
    , cast(NULL as date) as procedure_date_12
    , cast(NULL as date) as procedure_date_13
    , cast(NULL as date) as procedure_date_14
    , cast(NULL as date) as procedure_date_15
    , cast(NULL as date) as procedure_date_16
    , cast(NULL as date) as procedure_date_17
    , cast(NULL as date) as procedure_date_18
    , cast(NULL as date) as procedure_date_19
    , cast(NULL as date) as procedure_date_20
    , cast(NULL as date) as procedure_date_21
    , cast(NULL as date) as procedure_date_22
    , cast(NULL as date) as procedure_date_23
    , cast(NULL as date) as procedure_date_24
    , cast(NULL as date) as procedure_date_25
    , cast(1 as int) as in_network_flag
    , cast('medicare_lds' as {{ dbt.type_string() }} ) as data_source
    , cast(NULL as {{ dbt.type_string() }} ) as file_name
    , cast(NULL as {{ dbt.type_timestamp() }} ) as ingest_datetime
from hospice_base_claim as b
    inner join {{ ref('stg_hospice_revenue_center') }} as l
        on b.claim_no = l.claim_no
    /* Payment is provided at the header level only.  Populating on revenue center 001 to avoid duplication. */
    left join header_payment as p
        on b.claim_no = p.claim_id
    left join claim_start_date as c
        on b.claim_no = c.claim_no