select
    cast(null as {{ dbt.type_string() }}) as CLAIM_ID
    ,cast(null as {{ dbt.type_string() }}) as CLAIM_LINE_NUMBER
    ,cast(null as {{ dbt.type_string() }}) as PATIENT_ID
    ,cast(null as {{ dbt.type_string() }}) as PERSON_ID
    ,cast(null as {{ dbt.type_string() }}) as MEMBER_ID
    ,cast(null as {{ dbt.type_string() }}) as PAYER
    ,cast(null as {{ dbt.type_string() }}) as PLAN
    ,cast(null as {{ dbt.type_string() }}) as PRESCRIBING_PROVIDER_NPI
    ,cast(null as {{ dbt.type_string() }}) as DISPENSING_PROVIDER_NPI
    ,cast(null as date ) as DISPENSING_DATE
    ,cast(null as {{ dbt.type_string() }}) as NDC_CODE
    ,cast(null as int) as QUANTITY
    ,cast(null as int) as DAYS_SUPPLY
    ,cast(null as int) as REFILLS
    ,cast(null as date) as PAID_DATE
    ,cast(null as numeric) as PAID_AMOUNT
    ,cast(null as numeric) as ALLOWED_AMOUNT
    ,cast(null as numeric) as CHARGE_AMOUNT
    ,cast(null as numeric) as COINSURANCE_AMOUNT
    ,cast(null as numeric) as COPAYMENT_AMOUNT
    ,cast(null as numeric) as DEDUCTIBLE_AMOUNT
    ,cast(null as numeric) as IN_NETWORK_FLAG
    ,cast(null as {{ dbt.type_string() }}) as FILE_NAME
    ,cast(null as date) as INGEST_DATETIME
    ,cast(null as {{ dbt.type_string() }}) as DATA_SOURCE
limit 0