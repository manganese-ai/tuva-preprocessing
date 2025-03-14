{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

select distinct
    med.claim_id
  , med.claim_line_number
  , med.claim_type
  , med.data_source
  , rend_prov.npi as normalized_rendering_npi
  , case 
      when rend_prov.entity_type_code = '1' then 
        cast({{ dbt.concat(["rend_prov.provider_last_name", "', '", "rend_prov.provider_first_name"]) }} as {{ dbt.type_string() }})
      else 
        cast(rend_prov.provider_organization_name as {{ dbt.type_string() }})
    end as normalized_rendering_name
  , bill_prov.npi as normalized_billing_npi
  , case 
      when bill_prov.entity_type_code = '1' then
        cast({{ dbt.concat(["bill_prov.provider_last_name", "', '", "bill_prov.provider_first_name"]) }} as {{ dbt.type_string() }})
      else 
        cast(bill_prov.provider_organization_name as {{ dbt.type_string() }})
    end as normalized_billing_name
  , case 
      when med.claim_type = 'institutional' and fac_prov.entity_type_description = 'Organization'
      then fac_prov.npi
      else null
    end as normalized_facility_npi
  , case 
      when med.claim_type = 'institutional' and fac_prov.entity_type_description = 'Organization' then 
        case 
          when fac_prov.entity_type_code = '1' then 
            cast({{ dbt.concat(["fac_prov.provider_last_name", "', '", "fac_prov.provider_first_name"]) }} as {{ dbt.type_string() }})
          else 
            cast(fac_prov.provider_organization_name as {{ dbt.type_string() }})
        end
      else null
    end as normalized_facility_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} as med
left join {{ ref('terminology__provider') }} as rend_prov
  on med.rendering_npi = rend_prov.npi
left join {{ ref('terminology__provider') }} as bill_prov
  on med.billing_npi = bill_prov.npi
left join {{ ref('terminology__provider') }} as fac_prov
  on med.facility_npi = fac_prov.npi